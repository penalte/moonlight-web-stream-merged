use std::{
    collections::HashMap,
    io::{self, ErrorKind},
    path::PathBuf,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::join_all;
use moonlight_common::{crypto::rustcrypto::RustCryptoBackend, http::pair::PairingCryptoBackend};
use tokio::{
    fs,
    io::AsyncWriteExt,
    spawn,
    sync::{
        Mutex, RwLock,
        mpsc::{self, Receiver, Sender, error::TrySendError},
        oneshot,
    },
    task::JoinHandle,
    time::sleep,
};
use tracing::{debug, error};

use crate::app::{
    AppError,
    auth::SessionToken,
    host::HostId,
    password::StoragePassword,
    role::RoleId,
    storage::{
        Either, Storage, StorageHost, StorageHostAdd, StorageHostCache, StorageHostModify,
        StorageHostPairInfo, StorageOidcIdentity, StorageQueryHosts, StorageRole, StorageRoleAdd,
        StorageRoleDefaultSettings, StorageRoleModify, StorageRolePermissions, StorageUser,
        StorageUserAdd, StorageUserModify,
        json::versions::{
            Json, V2, V2Host, V2HostCache, V2HostPairInfo, V2UserPassword, V3, V3OidcIdentity,
            V3Role, V3RolePermissions, V3RoleType, V3User, migrate_to_latest,
        },
    },
    user::{RoleType, UserId},
};

mod serde_helpers;
mod versions;

pub struct JsonStorage {
    file: PathBuf,
    write_lock: Mutex<()>,
    store_sender: Sender<()>,
    session_expiration_checker: JoinHandle<()>,
    // IMPORTANT: only lock those mutexes in descending order to prevent deadlocks
    users: RwLock<HashMap<u32, RwLock<V3User>>>,
    hosts: RwLock<HashMap<u32, RwLock<V2Host>>>,
    roles: RwLock<HashMap<u32, RwLock<V3Role>>>,
    sessions: RwLock<HashMap<SessionToken, Session>>,
    default_role_id: RwLock<Option<u32>>,
    default_user_id: RwLock<Option<u32>>,
}

impl Drop for JsonStorage {
    fn drop(&mut self) {
        self.session_expiration_checker.abort();
    }
}

struct Session {
    created_at: Instant,
    expiration: Duration,
    user_id: u32,
}

impl JsonStorage {
    async fn insert_user(
        &self,
        user: StorageUserAdd,
        first_only: bool,
    ) -> Result<StorageUser, AppError> {
        let user = V3User {
            role_id: user.role_id.0,
            name: user.name,
            password: user.password.map(|password| V2UserPassword {
                salt: password.salt,
                hash: password.hash,
                iterations: password.iterations,
            }),
            client_unique_id: user.client_unique_id,
            oidc_identity: user.oidc_identity.map(|identity| V3OidcIdentity {
                issuer: identity.issuer,
                subject: identity.subject,
            }),
        };

        let mut users = self.users.write().await;
        if first_only && !users.is_empty() {
            return Err(AppError::FirstUserAlreadyExists);
        }
        let roles = self.roles.read().await;
        if !roles.contains_key(&user.role_id) {
            return Err(AppError::RoleNotFound);
        }

        // Enforce username and OIDC identity uniqueness while holding the users
        // write lock so concurrent provisioning cannot pass a check-then-insert race.
        for existing in users.values() {
            let existing = existing.read().await;
            let duplicate_name = existing.name == user.name;
            let duplicate_oidc_identity = user.oidc_identity.as_ref().is_some_and(|identity| {
                existing
                    .oidc_identity
                    .as_ref()
                    .is_some_and(|existing_identity| {
                        existing_identity.issuer == identity.issuer
                            && existing_identity.subject == identity.subject
                    })
            });
            if duplicate_name || duplicate_oidc_identity {
                return Err(AppError::UserAlreadyExists);
            }
        }

        let mut id;
        loop {
            id = random_number()?;

            if !users.contains_key(&id) {
                break;
            }
        }
        users.insert(id, RwLock::new(user.clone()));

        drop(users);

        self.force_write();

        Ok(StorageUser {
            id: UserId(id),
            name: user.name,
            password: user.password.map(|password| StoragePassword {
                salt: password.salt,
                hash: password.hash,
                iterations: password.iterations,
            }),
            role_id: RoleId(user.role_id),
            client_unique_id: user.client_unique_id,
            oidc_identity: user.oidc_identity.map(|identity| StorageOidcIdentity {
                issuer: identity.issuer,
                subject: identity.subject,
            }),
        })
    }

    pub async fn load(
        file: PathBuf,
        session_expiration_check_interval: Duration,
    ) -> Result<Arc<Self>, anyhow::Error> {
        let (store_sender, store_receiver) = mpsc::channel(1);

        let (this_sender, this_receiver) = oneshot::channel::<Weak<Self>>();

        let session_expiration_checker = spawn(async move {
            let this = match this_receiver.await {
                Ok(value) => value,
                Err(err) => {
                    error!(
                        "Failed to initialize session expiration checker: {err:?}. All sessions will last forever!"
                    );
                    return;
                }
            };

            loop {
                sleep(session_expiration_check_interval).await;
                let Some(this) = this.upgrade() else {
                    return;
                };
                debug!("Clearing all expired sessions!");

                let mut sessions = this.sessions.write().await;

                let now = Instant::now();
                sessions.retain(|_, session| {
                    let current_session_length = now - session.created_at;

                    current_session_length < session.expiration
                });
            }
        });

        let this = Self {
            file,
            write_lock: Mutex::new(()),
            store_sender,
            session_expiration_checker,
            hosts: Default::default(),
            users: Default::default(),
            roles: Default::default(),
            sessions: Default::default(),
            default_role_id: Default::default(),
            default_user_id: Default::default(),
        };
        let this = Arc::new(this);

        if this_sender.send(Arc::downgrade(&this)).is_err() {
            error!(
                "Failed to send values to session expiration checker. All sessions will last forever!"
            );
        }

        this.load_internal().await?;

        spawn({
            let this = Arc::downgrade(&this);

            async move { file_writer(store_receiver, this).await }
        });

        Ok(this)
    }

    pub fn force_write(&self) {
        if let Err(TrySendError::Closed(_)) = self.store_sender.try_send(()) {
            error!("Failed to save data because the writer task closed!");
        }
    }

    async fn load_internal(&self) -> Result<(), anyhow::Error> {
        let text = match fs::read_to_string(&self.file).await {
            Ok(text) => text,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return Ok(());
            }
            Err(err) => {
                return Err(anyhow!("Failed to read data: {err:?}"));
            }
        };

        let json = match serde_json::from_str::<Json>(&text) {
            Ok(value) => value,
            Err(err) => {
                let error = serde_json::from_str::<V2>(&text)
                    .err()
                    .map(|x| x.to_string())
                    .unwrap_or("none".to_string());

                return Err(anyhow!(
                    "Failed to deserialize data as json: {err}, Version specific error: {error}"
                ));
            }
        };

        let data = migrate_to_latest(json)?;

        {
            let mut users = self.users.write().await;
            let mut hosts = self.hosts.write().await;
            let mut roles = self.roles.write().await;
            let mut default_role_id = self.default_role_id.write().await;
            let mut default_user_id = self.default_user_id.write().await;

            *users = data
                .users
                .into_iter()
                .map(|(id, user)| (id, RwLock::new(user)))
                .collect();
            *hosts = data
                .hosts
                .into_iter()
                .map(|(id, host)| (id, RwLock::new(host)))
                .collect();
            *roles = data
                .roles
                .into_iter()
                .map(|(id, role)| (id, RwLock::new(role)))
                .collect();
            *default_role_id = data.default_role_id;
            *default_user_id = data.default_user_id;
        }

        Ok(())
    }
    async fn store(&self) -> io::Result<()> {
        let _write = self.write_lock.lock().await;
        let json = {
            let users = self.users.read().await;
            let hosts = self.hosts.read().await;
            let roles = self.roles.read().await;
            let default_role_id = self.default_role_id.read().await;
            let default_user_id = self.default_user_id.read().await;

            let mut users_json = HashMap::new();
            for (key, value) in users.iter() {
                let value = value.read().await;

                users_json.insert(*key, (*value).clone());
            }

            let mut hosts_json = HashMap::new();
            for (key, value) in hosts.iter() {
                let value = value.read().await;

                hosts_json.insert(*key, (*value).clone());
            }

            let mut roles_json = HashMap::new();
            for (key, value) in roles.iter() {
                let value = value.read().await;

                roles_json.insert(*key, (*value).clone());
            }

            Json::V3(V3 {
                users: users_json,
                hosts: hosts_json,
                roles: roles_json,
                default_role_id: *default_role_id,
                default_user_id: *default_user_id,
            })
        };

        let text = serde_json::to_vec_pretty(&json).map_err(io::Error::other)?;
        let parent = self
            .file
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        fs::create_dir_all(parent).await?;
        // The writer lock covers snapshot creation and replacement, so an older
        // queued save cannot overwrite a newer flush. Keep the temp on this volume.
        let temporary = self.file.with_extension("json.tmp");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temporary)
            .await?;
        file.write_all(&text).await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(&temporary, &self.file).await?;
        #[cfg(unix)]
        fs::File::open(parent).await?.sync_all().await?;
        Ok(())
    }
}

async fn file_writer(mut store_receiver: Receiver<()>, json: Weak<JsonStorage>) {
    loop {
        if store_receiver.recv().await.is_none() {
            return;
        }

        let Some(json) = json.upgrade() else {
            return;
        };
        if let Err(err) = json.store().await {
            error!(error = %err, "Failed to persist application data");
        }
    }
}

fn permissions_from_json(permissions: V3RolePermissions) -> StorageRolePermissions {
    StorageRolePermissions {
        allow_add_hosts: permissions.allow_add_hosts,
        maximum_bitrate_kbps: permissions.maximum_bitrate_kbps,
        allow_codec_h264: permissions.allow_codec_h264,
        allow_codec_h265: permissions.allow_codec_h265,
        allow_codec_av1: permissions.allow_codec_av1,
        allow_hdr: permissions.allow_hdr,
        allow_transport_webrtc: permissions.allow_transport_webrtc,
        allow_transport_websockets: permissions.allow_transport_websockets,
    }
}
fn permissions_to_json(permissions: StorageRolePermissions) -> V3RolePermissions {
    V3RolePermissions {
        allow_add_hosts: permissions.allow_add_hosts,
        maximum_bitrate_kbps: permissions.maximum_bitrate_kbps,
        allow_codec_h264: permissions.allow_codec_h264,
        allow_codec_h265: permissions.allow_codec_h265,
        allow_codec_av1: permissions.allow_codec_av1,
        allow_hdr: permissions.allow_hdr,
        allow_transport_webrtc: permissions.allow_transport_webrtc,
        allow_transport_websockets: permissions.allow_transport_websockets,
    }
}

fn role_from_json(role_id: RoleId, role: &V3Role) -> StorageRole {
    StorageRole {
        id: role_id,
        name: role.name.clone(),
        ty: match role.ty {
            V3RoleType::Admin => RoleType::Admin,
            V3RoleType::User => RoleType::User,
        },
        default_settings: StorageRoleDefaultSettings {
            value: role.default_settings.clone(),
        },
        permissions: permissions_from_json(role.permissions.clone()),
    }
}

fn user_from_json(user_id: UserId, user: &V3User) -> StorageUser {
    StorageUser {
        id: user_id,
        name: user.name.clone(),
        password: user.password.as_ref().map(|password| StoragePassword {
            salt: password.salt,
            hash: password.hash,
            iterations: password.iterations,
        }),
        role_id: RoleId(user.role_id),
        client_unique_id: user.client_unique_id.clone(),
        oidc_identity: user
            .oidc_identity
            .as_ref()
            .map(|identity| StorageOidcIdentity {
                issuer: identity.issuer.clone(),
                subject: identity.subject.clone(),
            }),
    }
}

fn host_from_json(host_id: HostId, host: &V2Host) -> StorageHost {
    StorageHost {
        id: host_id,
        owner: host.owner.map(UserId),
        address: host.address.clone(),
        http_port: host.http_port,
        pair_info: host.pair_info.clone().map(|pair_info| StorageHostPairInfo {
            client_certificate: pair_info.client_certificate,
            client_private_key: pair_info.client_private_key,
            server_certificate: pair_info.server_certificate,
        }),
        cache: StorageHostCache {
            name: host.cache.name.clone(),
            mac: host.cache.mac,
        },
    }
}

fn random_number() -> Result<u32, AppError> {
    let mut id_bytes = [0u8; 4];
    RustCryptoBackend.random_bytes(&mut id_bytes)?;
    Ok(u32::from_be_bytes(id_bytes))
}

#[async_trait]
impl Storage for JsonStorage {
    async fn flush(&self) -> Result<(), AppError> {
        self.store().await.map_err(AppError::from)
    }
    async fn add_role(&self, role: StorageRoleAdd) -> Result<StorageRole, AppError> {
        let role = V3Role {
            ty: match role.ty {
                RoleType::Admin => V3RoleType::Admin,
                RoleType::User => V3RoleType::User,
            },
            name: role.name,
            default_settings: role.default_settings.value,
            permissions: permissions_to_json(role.permissions),
        };

        let mut roles = self.roles.write().await;

        let mut id;
        loop {
            id = random_number()?;

            if !roles.contains_key(&id) {
                break;
            }
        }
        roles.insert(id, RwLock::new(role.clone()));

        drop(roles);

        self.force_write();

        Ok(StorageRole {
            ty: match role.ty {
                V3RoleType::Admin => RoleType::Admin,
                V3RoleType::User => RoleType::User,
            },
            id: RoleId(id),
            name: role.name,
            default_settings: StorageRoleDefaultSettings {
                value: role.default_settings,
            },
            permissions: permissions_from_json(role.permissions),
        })
    }
    async fn modify_role(
        &self,
        role_id: RoleId,
        modify: StorageRoleModify,
    ) -> Result<(), AppError> {
        let roles = self.roles.read().await;

        let role_lock = roles.get(&role_id.0).ok_or(AppError::RoleNotFound)?;
        let mut role = role_lock.write().await;

        if let Some(name) = modify.name {
            role.name = name;
        }
        if let Some(ty) = modify.ty {
            role.ty = match ty {
                RoleType::Admin => V3RoleType::Admin,
                RoleType::User => V3RoleType::User,
            };
        }
        if let Some(StorageRoleDefaultSettings { value }) = modify.default_settings {
            role.default_settings = value;
        }
        if let Some(StorageRolePermissions {
            allow_add_hosts,
            maximum_bitrate_kbps,
            allow_codec_h264,
            allow_codec_h265,
            allow_codec_av1,
            allow_hdr,
            allow_transport_webrtc,
            allow_transport_websockets,
        }) = modify.permissions
        {
            role.permissions.allow_add_hosts = allow_add_hosts;
            role.permissions.maximum_bitrate_kbps = maximum_bitrate_kbps;
            role.permissions.allow_codec_h264 = allow_codec_h264;
            role.permissions.allow_codec_h265 = allow_codec_h265;
            role.permissions.allow_codec_av1 = allow_codec_av1;
            role.permissions.allow_hdr = allow_hdr;
            role.permissions.allow_transport_webrtc = allow_transport_webrtc;
            role.permissions.allow_transport_websockets = allow_transport_websockets;
        }

        drop(role);
        drop(roles);

        self.force_write();

        Ok(())
    }
    async fn get_role(&self, role_id: RoleId) -> Result<StorageRole, AppError> {
        let roles = self.roles.read().await;

        let role_lock = roles.get(&role_id.0).ok_or(AppError::RoleNotFound)?;
        let role = role_lock.read().await;

        Ok(role_from_json(role_id, &role))
    }
    async fn remove_role(&self, role_id: RoleId) -> Result<(), AppError> {
        let users = self.users.write().await;
        let mut roles = self.roles.write().await;
        if !roles.contains_key(&role_id.0) {
            return Err(AppError::RoleNotFound);
        }
        for user in users.values() {
            if user.read().await.role_id == role_id.0 {
                return Err(AppError::RoleInUse);
            }
        }
        roles.remove(&role_id.0);
        let mut default_role_id = self.default_role_id.write().await;
        if *default_role_id == Some(role_id.0) {
            *default_role_id = None;
        }
        self.force_write();
        Ok(())
    }
    async fn list_roles(&self) -> Result<Either<Vec<RoleId>, Vec<StorageRole>>, AppError> {
        let roles = self.roles.read().await;

        let futures = roles.iter().map(|(id, value)| {
            let id = *id;
            async move {
                let role = value.read().await.clone();
                role_from_json(RoleId(id), &role)
            }
        });

        let out = join_all(futures).await;
        Ok(Either::Right(out))
    }
    async fn default_role(&self) -> Result<Option<Either<RoleId, StorageRole>>, AppError> {
        let default_role_id = self.default_role_id.read().await;

        Ok(default_role_id.map(RoleId).map(Either::Left))
    }
    async fn set_default_role(&self, role_id: Option<RoleId>) -> Result<(), AppError> {
        let roles = self.roles.read().await;
        if let Some(role_id) = role_id
            && !roles.contains_key(&role_id.0)
        {
            return Err(AppError::RoleNotFound);
        }
        let mut default_role_id = self.default_role_id.write().await;

        *default_role_id = role_id.map(|x| x.0);

        self.force_write();

        Ok(())
    }

    async fn add_user(&self, user: StorageUserAdd) -> Result<StorageUser, AppError> {
        self.insert_user(user, false).await
    }
    async fn add_first_user(&self, user: StorageUserAdd) -> Result<StorageUser, AppError> {
        self.insert_user(user, true).await
    }
    async fn modify_user(
        &self,
        user_id: UserId,
        modify: StorageUserModify,
    ) -> Result<(), AppError> {
        let users = self.users.read().await;

        let user_lock = users.get(&user_id.0).ok_or(AppError::UserNotFound)?;
        if let Some(role_id) = modify.role_id
            && !self.roles.read().await.contains_key(&role_id.0)
        {
            return Err(AppError::RoleNotFound);
        }
        let mut user = user_lock.write().await;

        if let Some(password) = modify.password {
            self.remove_all_user_session_tokens(user_id).await?;
            user.password = password.map(|password| V2UserPassword {
                salt: password.salt,
                hash: password.hash,
                iterations: password.iterations,
            });
        }
        if let Some(role_id) = modify.role_id {
            user.role_id = role_id.0;
        }
        if let Some(client_unique_id) = modify.client_unique_id {
            user.client_unique_id = client_unique_id;
        }
        if let Some(oidc_identity) = modify.oidc_identity {
            user.oidc_identity = oidc_identity.map(|identity| V3OidcIdentity {
                issuer: identity.issuer,
                subject: identity.subject,
            });
        }

        drop(user);
        drop(users);

        self.force_write();

        Ok(())
    }
    async fn get_user(&self, user_id: UserId) -> Result<StorageUser, AppError> {
        let users = self.users.read().await;

        let user_lock = users.get(&user_id.0).ok_or(AppError::UserNotFound)?;
        let user = user_lock.read().await;

        Ok(user_from_json(user_id, &user))
    }
    async fn get_user_by_name(
        &self,
        name: &str,
    ) -> Result<(UserId, Option<StorageUser>), AppError> {
        let users = self.users.read().await;

        let results = join_all(users.iter().map(|(user_id, user)| async move {
            let user = user.read().await;

            let user_id = UserId(*user_id);
            let user = (user.name == name).then(|| user_from_json(user_id, &user));

            (user_id, user)
        }))
        .await;

        let user = results.into_iter().find(|(_, user)| user.is_some());

        user.ok_or(AppError::UserNotFound)
    }
    async fn get_user_by_oidc_identity(
        &self,
        issuer: &str,
        subject: &str,
    ) -> Result<(UserId, Option<StorageUser>), AppError> {
        let users = self.users.read().await;

        let results = join_all(users.iter().map(|(user_id, user)| async move {
            let user = user.read().await;

            let user_id = UserId(*user_id);
            let user = user
                .oidc_identity
                .as_ref()
                .is_some_and(|identity| identity.issuer == issuer && identity.subject == subject)
                .then(|| user_from_json(user_id, &user));

            (user_id, user)
        }))
        .await;

        let user = results.into_iter().find(|(_, user)| user.is_some());

        user.ok_or(AppError::UserNotFound)
    }
    async fn remove_user(&self, user_id: UserId) -> Result<(), AppError> {
        let mut users = self.users.write().await;

        let result = match users.remove(&user_id.0) {
            None => Err(AppError::UserNotFound),
            Some(_) => Ok(()),
        };

        self.remove_all_user_session_tokens(user_id).await?;
        let mut default_user_id = self.default_user_id.write().await;
        if *default_user_id == Some(user_id.0) {
            *default_user_id = None;
        }
        drop(users);

        self.force_write();

        result
    }
    async fn list_users(&self) -> Result<Either<Vec<UserId>, Vec<StorageUser>>, AppError> {
        let users = self.users.read().await;

        let futures = users.iter().map(|(id, value)| {
            let id = *id;
            async move {
                let user = value.read().await.clone();
                user_from_json(UserId(id), &user)
            }
        });

        let out = join_all(futures).await;
        Ok(Either::Right(out))
    }
    async fn any_user_exists(&self) -> Result<bool, AppError> {
        let users = self.users.read().await;

        Ok(!users.is_empty())
    }

    async fn default_user(&self) -> Result<Option<Either<UserId, StorageUser>>, AppError> {
        let default_user_id = self.default_user_id.read().await;

        Ok(default_user_id.map(UserId).map(Either::Left))
    }
    async fn set_default_user(&self, user_id: Option<UserId>) -> Result<(), AppError> {
        let users = self.users.read().await;
        if let Some(user_id) = user_id
            && !users.contains_key(&user_id.0)
        {
            return Err(AppError::UserNotFound);
        }
        let mut default_user_id = self.default_user_id.write().await;

        *default_user_id = user_id.map(|x| x.0);

        self.force_write();

        Ok(())
    }

    async fn create_session_token(
        &self,
        user_id: UserId,
        expiration: Duration,
    ) -> Result<SessionToken, AppError> {
        let users = self.users.read().await;
        let user = users.get(&user_id.0).ok_or(AppError::UserNotFound)?;
        let _user = user.read().await;
        let mut token;
        {
            let sessions = self.sessions.read().await;

            loop {
                token = SessionToken::new()?;
                if !sessions.contains_key(&token) {
                    break;
                }
            }
        };

        let mut sessions = self.sessions.write().await;

        sessions.insert(
            token,
            Session {
                created_at: Instant::now(),
                expiration,
                user_id: user_id.0,
            },
        );

        Ok(token)
    }
    async fn remove_session_token(&self, session: SessionToken) -> Result<(), AppError> {
        let mut sessions = self.sessions.write().await;

        sessions.remove(&session);

        Ok(())
    }
    async fn remove_all_user_session_tokens(&self, user_id: UserId) -> Result<(), AppError> {
        let mut sessions = self.sessions.write().await;

        sessions.retain(|_, session| UserId(session.user_id) != user_id);

        Ok(())
    }
    async fn get_user_by_session_token(
        &self,
        session: SessionToken,
    ) -> Result<(UserId, Option<StorageUser>), AppError> {
        let sessions = self.sessions.read().await;

        sessions
            .get(&session)
            .filter(|session| session.created_at.elapsed() < session.expiration)
            .map(|session| (UserId(session.user_id), None))
            .ok_or(AppError::SessionTokenNotFound)
    }

    async fn add_host(&self, host: StorageHostAdd) -> Result<StorageHost, AppError> {
        let host = V2Host {
            owner: host.owner.map(|user_id| user_id.0),
            address: host.address,
            http_port: host.http_port,
            pair_info: host.pair_info.map(|pair_info| V2HostPairInfo {
                client_private_key: pair_info.client_private_key,
                client_certificate: pair_info.client_certificate,
                server_certificate: pair_info.server_certificate,
            }),
            cache: V2HostCache {
                name: host.cache.name,
                mac: host.cache.mac,
            },
        };

        let mut hosts = self.hosts.write().await;

        let mut id;
        loop {
            id = random_number()?;

            if !hosts.contains_key(&id) {
                break;
            }
        }
        hosts.insert(id, RwLock::new(host.clone()));

        self.force_write();

        Ok(StorageHost {
            id: HostId(id),
            owner: host.owner.map(UserId),
            address: host.address,
            http_port: host.http_port,
            pair_info: host.pair_info.map(|pair_info| StorageHostPairInfo {
                client_private_key: pair_info.client_private_key,
                client_certificate: pair_info.client_certificate,
                server_certificate: pair_info.server_certificate,
            }),
            cache: StorageHostCache {
                name: host.cache.name,
                mac: host.cache.mac,
            },
        })
    }
    async fn modify_host(
        &self,
        host_id: HostId,
        modify: StorageHostModify,
    ) -> Result<(), AppError> {
        let hosts = self.hosts.read().await;

        let host = hosts.get(&host_id.0).ok_or(AppError::HostNotFound)?;
        let mut host = host.write().await;

        if let Some(new_owner) = modify.owner {
            host.owner = new_owner.map(|user_id| user_id.0);
        }
        if let Some(new_address) = modify.address {
            host.address = new_address;
        }
        if let Some(new_http_port) = modify.http_port {
            host.http_port = new_http_port;
        }
        if let Some(new_pair_info) = modify.pair_info {
            host.pair_info = new_pair_info.map(|new_pair_info| V2HostPairInfo {
                client_private_key: new_pair_info.client_private_key,
                client_certificate: new_pair_info.client_certificate,
                server_certificate: new_pair_info.server_certificate,
            });
        }
        if let Some(new_cache_name) = modify.cache_name {
            host.cache.name = new_cache_name;
        }
        if let Some(new_cache_mac) = modify.cache_mac {
            host.cache.mac = new_cache_mac;
        }

        self.force_write();

        Ok(())
    }
    async fn get_host(&self, host_id: HostId) -> Result<StorageHost, AppError> {
        let hosts = self.hosts.read().await;

        let host = hosts.get(&host_id.0).ok_or(AppError::HostNotFound)?;
        let host = host.read().await;

        Ok(host_from_json(host_id, &host))
    }
    async fn remove_host(&self, host_id: HostId) -> Result<(), AppError> {
        let mut hosts = self.hosts.write().await;

        if hosts.remove(&host_id.0).is_none() {
            return Err(AppError::HostNotFound);
        }

        self.force_write();

        Ok(())
    }

    async fn list_user_hosts(
        &self,
        query: StorageQueryHosts,
    ) -> Result<Vec<(HostId, Option<StorageHost>)>, AppError> {
        let hosts = self.hosts.read().await;

        let mut user_hosts = Vec::new();
        for (host_id, host) in &*hosts {
            let host_id = HostId(*host_id);
            let host = host.read().await;

            if host.owner.is_none() || host.owner.map(UserId) == Some(query.user_id) {
                user_hosts.push((host_id, Some(host_from_json(host_id, &host))));
            }
        }

        Ok(user_hosts)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod regression_tests {
    use super::*;

    async fn storage() -> Arc<JsonStorage> {
        let file = std::env::temp_dir().join(format!(
            "moonlight-storage-regression-{}.json",
            uuid::Uuid::new_v4()
        ));
        JsonStorage::load(file, Duration::from_secs(3600))
            .await
            .unwrap()
    }

    async fn role(storage: &JsonStorage) -> RoleId {
        storage
            .add_role(StorageRoleAdd {
                name: "Test".into(),
                ty: RoleType::User,
                default_settings: StorageRoleDefaultSettings::default(),
                permissions: StorageRolePermissions::default(),
            })
            .await
            .unwrap()
            .id
    }

    fn user(role_id: RoleId, name: &str) -> StorageUserAdd {
        StorageUserAdd {
            role_id,
            name: name.into(),
            password: None,
            client_unique_id: name.into(),
            oidc_identity: None,
        }
    }

    #[tokio::test]
    async fn concurrent_bootstrap_creates_only_one_user() {
        let storage = storage().await;
        let role_id = role(&storage).await;
        let (first, second) = tokio::join!(
            storage.add_first_user(user(role_id, "first")),
            storage.add_first_user(user(role_id, "second")),
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert!(
            matches!(first, Err(AppError::FirstUserAlreadyExists))
                || matches!(second, Err(AppError::FirstUserAlreadyExists))
        );
        assert_eq!(storage.users.read().await.len(), 1);
    }

    #[tokio::test]
    async fn changing_password_revokes_all_existing_sessions_only_for_that_user() {
        let storage = storage().await;
        let role_id = role(&storage).await;
        let alice = storage.add_user(user(role_id, "alice")).await.unwrap();
        let bob = storage.add_user(user(role_id, "bob")).await.unwrap();
        let first = storage
            .create_session_token(alice.id, Duration::from_secs(60))
            .await
            .unwrap();
        let second = storage
            .create_session_token(alice.id, Duration::from_secs(60))
            .await
            .unwrap();
        let other = storage
            .create_session_token(bob.id, Duration::from_secs(60))
            .await
            .unwrap();
        storage
            .modify_user(
                alice.id,
                StorageUserModify {
                    password: Some(Some(StoragePassword {
                        salt: [1; 16],
                        hash: [2; 32],
                        iterations: 600_000,
                    })),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(
            storage.get_user_by_session_token(first).await,
            Err(AppError::SessionTokenNotFound)
        ));
        assert!(matches!(
            storage.get_user_by_session_token(second).await,
            Err(AppError::SessionTokenNotFound)
        ));
        assert!(storage.get_user_by_session_token(other).await.is_ok());
    }

    #[tokio::test]
    async fn expired_sessions_are_rejected_before_the_cleanup_task_runs() {
        let storage = storage().await;
        let id = storage
            .add_user(user(role(&storage).await, "alice"))
            .await
            .unwrap()
            .id;
        let token = storage
            .create_session_token(id, Duration::ZERO)
            .await
            .unwrap();
        assert!(storage.sessions.read().await.contains_key(&token));
        assert!(matches!(
            storage.get_user_by_session_token(token).await,
            Err(AppError::SessionTokenNotFound)
        ));
    }

    #[tokio::test]
    async fn deleting_a_populated_role_preserves_users_and_the_default() {
        let storage = storage().await;
        let role_id = role(&storage).await;
        let id = storage.add_user(user(role_id, "alice")).await.unwrap().id;
        storage.set_default_role(Some(role_id)).await.unwrap();
        assert!(matches!(
            storage.remove_role(role_id).await,
            Err(AppError::RoleInUse)
        ));
        assert!(storage.get_user(id).await.is_ok());
        assert!(storage.get_role(role_id).await.is_ok());
        storage.remove_user(id).await.unwrap();
        storage.remove_role(role_id).await.unwrap();
        assert!(storage.default_role().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn deleted_users_lose_sessions_and_default_selection() {
        let storage = storage().await;
        let id = storage
            .add_user(user(role(&storage).await, "alice"))
            .await
            .unwrap()
            .id;
        storage.set_default_user(Some(id)).await.unwrap();
        let token = storage
            .create_session_token(id, Duration::from_secs(60))
            .await
            .unwrap();
        storage.remove_user(id).await.unwrap();
        assert!(storage.default_user().await.unwrap().is_none());
        assert!(storage.get_user_by_session_token(token).await.is_err());
        assert!(
            storage
                .create_session_token(id, Duration::from_secs(60))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn flush_persists_latest_state_and_replaces_an_existing_file() {
        let storage = storage().await;
        let role_id = role(&storage).await;
        storage.flush().await.unwrap();
        let id = storage.add_user(user(role_id, "alice")).await.unwrap().id;
        storage.flush().await.unwrap();
        let loaded = JsonStorage::load(storage.file.clone(), Duration::from_secs(3600))
            .await
            .unwrap();
        assert_eq!(loaded.get_user(id).await.unwrap().name, "alice");
        let json = fs::read_to_string(&storage.file).await.unwrap();
        assert!(serde_json::from_str::<Json>(&json).is_ok());
    }
}
