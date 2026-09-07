use crate::api::{
    bindings::{
        DeleteRoleQuery, GetDefaultRoleResponse, GetRoleQuery, GetRoleResponse, GetRolesResponse,
        PatchRoleRequest, PostRoleRequest, PostRoleResponse, PutDefaultRoleRequest,
        StreamPermissions,
    },
    bindings_ext::TsAny,
};
use actix_web::{
    HttpResponse, delete, get, patch, post, put,
    web::{Data, Json, Query},
};

use futures::future::join_all;
use tracing::warn;

use crate::app::{
    App, AppError,
    role::RoleId,
    storage::{
        StorageRoleAdd, StorageRoleDefaultSettings, StorageRoleModify, StorageRolePermissions,
    },
    user::{Admin, AuthenticatedUser},
};

fn convert_settings(settings: TsAny) -> StorageRoleDefaultSettings {
    StorageRoleDefaultSettings {
        value: settings.into(),
    }
}
fn convert_permissions(permissions: StreamPermissions) -> StorageRolePermissions {
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

#[post("/role")]
pub async fn add_role(
    app: Data<App>,
    admin: Admin,
    Json(request): Json<PostRoleRequest>,
) -> Result<Json<PostRoleResponse>, AppError> {
    let PostRoleRequest {
        name,
        ty,
        default_settings,
        permissions,
    } = request;

    let mut role = app
        .add_role(
            &admin,
            StorageRoleAdd {
                name,
                ty: ty.into(),
                default_settings: convert_settings(default_settings),
                permissions: convert_permissions(permissions),
            },
        )
        .await?;

    Ok(Json(PostRoleResponse {
        role: role.detailed_role().await?,
    }))
}

#[get("/role")]
pub async fn get_role(
    app: Data<App>,
    mut user: AuthenticatedUser,
    Query(query): Query<GetRoleQuery>,
) -> Result<Json<GetRoleResponse>, AppError> {
    let role_id = match query.id {
        Some(id) => RoleId(id),
        None => user.role_id().await?,
    };

    let mut role = app.role_by_id(role_id).await?;

    Ok(Json(GetRoleResponse {
        role: role.detailed_role().await?,
    }))
}

#[patch("/role")]
pub async fn patch_role(
    app: Data<App>,
    admin: Admin,
    Json(request): Json<PatchRoleRequest>,
) -> Result<HttpResponse, AppError> {
    let role_id = RoleId(request.id);

    let role = app.role_by_id(role_id).await?;

    role.modify(
        &admin,
        StorageRoleModify {
            name: request.name,
            ty: request.ty.map(Into::into),
            permissions: request
                .permissions
                .map(|permissions| StorageRolePermissions {
                    allow_add_hosts: permissions.allow_add_hosts,
                    maximum_bitrate_kbps: permissions.maximum_bitrate_kbps,
                    allow_codec_h264: permissions.allow_codec_h264,
                    allow_codec_h265: permissions.allow_codec_h265,
                    allow_codec_av1: permissions.allow_codec_av1,
                    allow_hdr: permissions.allow_hdr,
                    allow_transport_webrtc: permissions.allow_transport_webrtc,
                    allow_transport_websockets: permissions.allow_transport_websockets,
                }),
            default_settings: request
                .default_settings
                .map(|settings| StorageRoleDefaultSettings {
                    value: settings.into(),
                }),
        },
    )
    .await?;

    Ok(HttpResponse::Ok().finish())
}

#[delete("/role")]
pub async fn delete_role(
    app: Data<App>,
    admin: Admin,
    Query(query): Query<DeleteRoleQuery>,
) -> Result<HttpResponse, AppError> {
    let role_id = RoleId(query.id);

    let role = app.role_by_id(role_id).await?;

    role.delete(&admin).await?;

    Ok(HttpResponse::Ok().finish())
}

#[get("/roles")]
pub async fn list_roles(app: Data<App>, admin: Admin) -> Result<Json<GetRolesResponse>, AppError> {
    let mut roles = app.all_roles(&admin).await?;

    let role_results = join_all(roles.iter_mut().map(|role| role.undetailed_role())).await;

    let mut out_roles = Vec::with_capacity(role_results.len());
    for (result, role) in role_results.into_iter().zip(roles) {
        match result {
            Ok(role) => {
                out_roles.push(role);
            }
            Err(err) => {
                warn!("Failed to query detailed role of {role:?}: {err}");
            }
        }
    }

    Ok(Json(GetRolesResponse { roles: out_roles }))
}

#[put("/role/default")]
pub async fn put_default_role(
    app: Data<App>,
    admin: Admin,
    Json(request): Json<PutDefaultRoleRequest>,
) -> Result<HttpResponse, AppError> {
    let role_id = RoleId(request.id);

    let role = app.role_by_id(role_id).await?;

    app.set_default_role(&admin, Some(&role)).await?;

    Ok(HttpResponse::Ok().finish())
}

#[delete("/role/default")]
pub async fn delete_default_role(app: Data<App>, admin: Admin) -> Result<HttpResponse, AppError> {
    app.set_default_role(&admin, None).await?;

    Ok(HttpResponse::Ok().finish())
}

#[get("/role/default")]
pub async fn get_default_role(
    app: Data<App>,
    _admin: Admin,
) -> Result<Json<GetDefaultRoleResponse>, AppError> {
    let role = app.default_role().await?;

    Ok(Json(GetDefaultRoleResponse { id: role.id().0 }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{
        api::api_service,
        app::user::RoleType,
        config::{Config, StorageConfig},
    };
    use actix_web::{App as ActixApp, http::StatusCode, test};
    use std::time::Duration;

    #[actix_web::test]
    async fn patch_role_applies_promotions_and_demotions() {
        let mut config = Config::default();
        config.data_storage = StorageConfig::Json {
            path: std::env::temp_dir()
                .join(format!("moonlight-role-test-{}.json", uuid::Uuid::new_v4()))
                .display()
                .to_string(),
            session_expiration_check_interval: Duration::from_secs(3600),
        };
        let app = Data::new(App::new(config).await.unwrap());
        let user = app
            .try_add_first_login("admin".into(), "test password".into())
            .await
            .unwrap();
        let token = user.new_session(Duration::from_secs(60)).await.unwrap();
        let mut bytes = [0; 64];
        let authorization = format!("Bearer {}", token.encode(&mut bytes));
        let admin = user.into_admin().await.unwrap();
        let role = app
            .add_role(
                &admin,
                StorageRoleAdd {
                    name: "Editable".into(),
                    ty: RoleType::User,
                    default_settings: StorageRoleDefaultSettings::default(),
                    permissions: StorageRolePermissions::default(),
                },
            )
            .await
            .unwrap();
        let service =
            test::init_service(ActixApp::new().app_data(app.clone()).service(api_service())).await;
        for (ty, expected) in [("Admin", RoleType::Admin), ("User", RoleType::User)] {
            let request = test::TestRequest::patch()
                .uri("/api/role")
                .insert_header(("Authorization", authorization.clone()))
                .set_json(serde_json::json!({ "id": role.id().0, "ty": ty }))
                .to_request();
            assert_eq!(
                test::call_service(&service, request).await.status(),
                StatusCode::OK
            );
            assert_eq!(
                app.role_by_id(role.id()).await.unwrap().ty().await.unwrap(),
                expected
            );
        }

        // A patch that omits ty must leave the role type alone. Sending it as a
        // required field would let any partial update silently demote a role.
        let request = test::TestRequest::patch()
            .uri("/api/role")
            .insert_header(("Authorization", authorization.clone()))
            .set_json(serde_json::json!({ "id": role.id().0, "name": "Renamed" }))
            .to_request();
        assert_eq!(
            test::call_service(&service, request).await.status(),
            StatusCode::OK
        );
        assert_eq!(
            app.role_by_id(role.id()).await.unwrap().ty().await.unwrap(),
            RoleType::User
        );
    }
}
