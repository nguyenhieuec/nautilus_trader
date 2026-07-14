// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Shared, client-scoped private-stream health state.

use std::sync::{Arc, RwLock};

use nautilus_core::{MUTEX_POISONED, UnixNanos};
use nautilus_model::{
    identifiers::ClientId,
    reports::{PrivateStreamHealth, PrivateStreamState},
};

/// Cloneable health handle shared by the client and its background tasks.
#[derive(Clone, Debug)]
pub(crate) struct PrivateStreamHealthHandle {
    inner: Arc<RwLock<PrivateStreamHealth>>,
}

impl PrivateStreamHealthHandle {
    pub(crate) fn new(client_id: ClientId) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PrivateStreamHealth {
                client_id,
                state: PrivateStreamState::Stale,
                generation: 0,
                authenticated: false,
                subscribed: false,
                last_heartbeat: None,
                detail: Some("not connected".to_string()),
            })),
        }
    }

    pub(crate) fn begin_reconnect(&self) -> u64 {
        let mut health = self.inner.write().expect(MUTEX_POISONED);
        health.generation = health.generation.saturating_add(1);
        health.state = PrivateStreamState::Reconciling;
        health.authenticated = false;
        health.subscribed = false;
        health.last_heartbeat = None;
        health.detail = Some("private stream generation changed".to_string());
        health.generation
    }

    pub(crate) fn authenticated(&self, generation: u64) {
        let mut health = self.inner.write().expect(MUTEX_POISONED);
        if health.generation == generation {
            health.authenticated = true;
        }
    }

    pub(crate) fn subscribed(&self, generation: u64) {
        let mut health = self.inner.write().expect(MUTEX_POISONED);
        if health.generation == generation {
            health.subscribed = true;
        }
    }

    pub(crate) fn ready(&self, generation: u64, now: UnixNanos) -> bool {
        let mut health = self.inner.write().expect(MUTEX_POISONED);
        if health.generation != generation
            || health.state != PrivateStreamState::Reconciling
            || !health.authenticated
            || !health.subscribed
        {
            return false;
        }
        health.state = PrivateStreamState::Ready;
        health.last_heartbeat = Some(now);
        health.detail = None;
        true
    }

    pub(crate) fn heartbeat(&self, generation: u64, now: UnixNanos) {
        let mut health = self.inner.write().expect(MUTEX_POISONED);
        if health.generation == generation {
            health.last_heartbeat = Some(now);
        }
    }

    pub(crate) fn stale(&self, generation: u64, detail: impl Into<String>) {
        let mut health = self.inner.write().expect(MUTEX_POISONED);
        if health.generation != generation {
            return;
        }
        health.state = PrivateStreamState::Stale;
        health.detail = Some(detail.into());
    }

    pub(crate) fn fail(&self, generation: u64, detail: impl Into<String>) {
        let mut health = self.inner.write().expect(MUTEX_POISONED);
        if health.generation != generation {
            return;
        }
        health.state = PrivateStreamState::Failed;
        health.detail = Some(detail.into());
    }

    pub(crate) fn current_generation(&self) -> u64 {
        self.inner.read().expect(MUTEX_POISONED).generation
    }

    pub(crate) fn snapshot(&self) -> PrivateStreamHealth {
        self.inner.read().expect(MUTEX_POISONED).clone()
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::reports::PrivateStreamState;

    use super::*;

    #[test]
    fn reconnect_invalidates_ready_generation() {
        let handle = PrivateStreamHealthHandle::new(ClientId::from("BINANCE-SPOT"));
        let first = handle.begin_reconnect();
        handle.authenticated(first);
        handle.subscribed(first);
        assert!(handle.ready(first, UnixNanos::from(1)));

        let second = handle.begin_reconnect();
        let health = handle.snapshot();
        assert_eq!(second, first + 1);
        assert_eq!(health.state, PrivateStreamState::Reconciling);
        assert!(!health.authenticated);
        assert!(!health.subscribed);
        assert!(!handle.ready(first, UnixNanos::from(2)));
    }

    #[test]
    fn failed_dispatch_generation_cannot_be_promoted() {
        let handle = PrivateStreamHealthHandle::new(ClientId::from("BINANCE-FUTURES"));
        let generation = handle.begin_reconnect();
        handle.authenticated(generation);
        handle.subscribed(generation);
        handle.fail(generation, "dispatch ended");

        assert!(!handle.ready(generation, UnixNanos::from(3)));
        assert_eq!(handle.snapshot().state, PrivateStreamState::Failed);
    }
}
