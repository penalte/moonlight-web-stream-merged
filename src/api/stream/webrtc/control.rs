use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use actix_web::web::Bytes;
use futures::future::{Either, pending};
use moonlight_common::stream::proto::control::packet::{
    ControlPacket, ControlPacketConfig, PacketDirection,
};
use tokio::select;
use tokio::sync::mpsc;

use tokio::sync::oneshot;
use tracing::{debug, info, warn};
use webrtc::data_channel::RTCDataChannel;
use webrtc::{
    data_channel::data_channel_state::RTCDataChannelState, peer_connection::RTCPeerConnection,
};

use crate::api::stream::create_control_packet_config;
use crate::app::AppError;

const QUEUE_CAPACITY: usize = 256;
const MAX_QUEUE_AGE: Duration = Duration::from_millis(250);
type QueuedPacket = (Instant, Bytes);

fn enqueue(sender: &mpsc::Sender<QueuedPacket>, failed: &AtomicBool, data: Bytes) {
    if failed.load(Ordering::Relaxed) {
        return;
    }
    if data.len() > ControlPacket::MAX_SIZE || sender.try_send((Instant::now(), data)).is_err() {
        warn!("WebRTC control queue overflow, closed, or oversized packet");
        failed.store(true, Ordering::Relaxed);
    }
}

pub enum ControlChannelEvent {
    Packet(ControlPacket),
    Closed,
}

pub struct ControlChannel {
    channel: Arc<RTCDataChannel>,
    on_open: oneshot::Receiver<()>,
    on_receive: mpsc::Receiver<QueuedPacket>,
    on_receive_sender: mpsc::Sender<QueuedPacket>,
    config: ControlPacketConfig,
    send_queue: VecDeque<QueuedPacket>,
    failed: Arc<AtomicBool>,
    send_stalled_since: Option<Instant>,
}

impl ControlChannel {
    pub async fn new(peer: &RTCPeerConnection) -> Result<Self, AppError> {
        let channel = peer.create_data_channel("moonlight.control", None).await?;

        let (send_open, on_open) = oneshot::channel();

        channel.on_open(Box::new(move || {
            Box::pin(async move {
                debug!("webrtc control channel opened");
                let _ = send_open.send(());
            })
        }));

        let (on_receive_sender, on_receive) = mpsc::channel(QUEUE_CAPACITY);
        let failed = Arc::new(AtomicBool::new(false));

        // The browser uses the primary control channel for reliable packets such
        // as mouse wheel input. Subchannels are only used for latency-sensitive
        // input, so the primary channel must receive messages too.
        let on_receive_sender_clone = on_receive_sender.clone();
        let receive_failed = failed.clone();
        channel.on_message(Box::new(move |message| {
            let on_receive_sender = on_receive_sender_clone.clone();
            let failed = receive_failed.clone();

            Box::pin(async move {
                enqueue(&on_receive_sender, &failed, message.data);
            })
        }));

        Ok(Self {
            channel,
            on_open,
            on_receive,
            on_receive_sender,
            config: create_control_packet_config(),
            send_queue: Default::default(),
            failed,
            send_stalled_since: None,
        })
    }

    pub fn try_add_channel(&mut self, channel: &RTCDataChannel) -> bool {
        if !channel.label().starts_with("moonlight.control.") {
            return false;
        }
        info!(label = %channel.label(), "adding control channel");

        let on_receive_sender = self.on_receive_sender.clone();
        let receive_failed = self.failed.clone();
        let close_failed = self.failed.clone();
        channel.on_close(Box::new(move || {
            let failed = close_failed.clone();
            Box::pin(async move {
                failed.store(true, Ordering::Relaxed);
            })
        }));
        channel.on_message(Box::new(move |message| {
            let send_receive = on_receive_sender.clone();
            let failed = receive_failed.clone();

            Box::pin(async move {
                enqueue(&send_receive, &failed, message.data);
            })
        }));

        true
    }

    pub fn send(&mut self, packet: ControlPacket) {
        let mut buffer = [0; ControlPacket::MAX_SIZE];

        let len = match packet.serialize(&self.config, &mut buffer) {
            Ok(value) => value,
            Err(err) => {
                warn!(error = %err, "failed to relay control packet from server to client");
                return;
            }
        };
        let buffer = &buffer[0..len];

        if self.send_queue.len() >= QUEUE_CAPACITY {
            warn!("WebRTC outbound control queue overflow");
            self.failed.store(true, Ordering::Relaxed);
            return;
        }
        self.send_queue
            .push_back((Instant::now(), Bytes::copy_from_slice(buffer)));
    }

    pub fn is_alive(&self) -> bool {
        !self.failed.load(Ordering::Relaxed)
            && !matches!(
                self.channel.ready_state(),
                RTCDataChannelState::Closed | RTCDataChannelState::Closing
            )
            && !self.on_receive.is_closed()
    }

    /// # Cancel Safety
    /// This function is cancel safe.
    /// If it is cancelled no state is lost.
    pub async fn drive(&mut self) -> Result<ControlChannelEvent, AppError> {
        loop {
            if !self.is_alive() {
                return Ok(ControlChannelEvent::Closed);
            }

            if !self.send_queue.is_empty()
                && self.channel.ready_state() == RTCDataChannelState::Open
            {
                let started = self.send_stalled_since.get_or_insert_with(Instant::now);
                if started.elapsed() > Duration::from_secs(1) {
                    warn!("WebRTC outbound control queue stalled");
                    return Ok(ControlChannelEvent::Closed);
                }
            } else {
                self.send_stalled_since = None;
            }
            let send_future = if let Some((_, transmit)) = self.send_queue.front()
                && matches!(self.channel.ready_state(), RTCDataChannelState::Open)
            {
                // This send function implementation seems cancel safe
                Either::Left(self.channel.send(transmit))
            } else {
                Either::Right(pending::<_>())
            };

            select! {
                result = self.on_receive.recv() => {
                    let Some((queued, packet)) = result else {
                        // The channel closed
                        return Ok(ControlChannelEvent::Closed);
                    };

                    if queued.elapsed() > MAX_QUEUE_AGE {
                        warn!(age_ms = queued.elapsed().as_millis(), "WebRTC input queue stalled; refusing stale input replay");
                        return Ok(ControlChannelEvent::Closed);
                    }
                    let Some(packet) = ControlPacket::deserialize(PacketDirection::ServerBound, &self.config, &packet) else {
                        warn!(packet = ?packet, "failed to deserialize packet from webrtc client");
                        continue;
                    };

                    return Ok(ControlChannelEvent::Packet(packet));
                },
                result = send_future => {
                    self.send_queue.pop_front();
                    self.send_stalled_since = None;

                    if let Err(err) = result {
                        warn!(error = %err, "failed to send packet on data channel");
                        return Ok(ControlChannelEvent::Closed);
                    }
                },
                // Check closure and queue deadlines even when no media or input wakes us.
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                // Wake up on channel open
                _ = &mut self.on_open, if !self.on_open.is_terminated() => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overload_fails_without_accepting_more_input() {
        let (sender, mut receiver) = mpsc::channel(2);
        let failed = AtomicBool::new(false);
        enqueue(&sender, &failed, Bytes::from_static(b"one"));
        enqueue(&sender, &failed, Bytes::from_static(b"two"));
        enqueue(&sender, &failed, Bytes::from_static(b"three"));
        assert!(failed.load(Ordering::Relaxed));
        assert_eq!(
            receiver.try_recv().map(|(_, bytes)| bytes).ok(),
            Some(Bytes::from_static(b"one"))
        );
        enqueue(&sender, &failed, Bytes::from_static(b"four"));
        assert_eq!(receiver.len(), 1);
    }
    #[test]
    fn oversized_input_is_rejected_before_queueing() {
        let (sender, receiver) = mpsc::channel(2);
        let failed = AtomicBool::new(false);
        enqueue(
            &sender,
            &failed,
            Bytes::from(vec![0; ControlPacket::MAX_SIZE + 1]),
        );
        assert!(failed.load(Ordering::Relaxed));
        assert_eq!(receiver.len(), 0);
    }
    #[tokio::test]
    async fn stale_input_and_closed_channels_report_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let peer = webrtc::api::APIBuilder::new()
            .build()
            .new_peer_connection(Default::default())
            .await?;
        let mut control = ControlChannel::new(&peer).await?;
        control.on_receive_sender.try_send((
            Instant::now() - Duration::from_secs(1),
            Bytes::from_static(b"stale"),
        ))?;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), control.drive()).await??,
            ControlChannelEvent::Closed
        ));
        control.channel.close().await?;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), control.drive()).await??,
            ControlChannelEvent::Closed
        ));
        peer.close().await?;
        Ok(())
    }
}
