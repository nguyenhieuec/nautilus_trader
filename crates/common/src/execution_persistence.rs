// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Opaque two-phase activation seam for persist-before-send execution writes.

use std::{fmt::Debug, sync::Arc};

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::identifiers::{ClientOrderId, InstrumentId};
use serde::{Deserialize, Serialize};

use crate::cache::database::{PersistenceError, PersistenceReceipt};

/// Opaque one-use authorization staged by an application before cache persistence.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionWriteStageToken(UUID4);

impl ExecutionWriteStageToken {
    /// Creates a new opaque stage token.
    #[must_use]
    pub fn new() -> Self {
        Self(UUID4::new())
    }
}

impl Default for ExecutionWriteStageToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Opaque stage token plus its strict activation deadline carried by a native command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StagedExecutionWrite {
    /// Opaque one-use stage token.
    pub token: ExecutionWriteStageToken,
    /// Deadline after which activation must fail.
    pub expires_at: UnixNanos,
}

/// Native command kinds which can result in a venue mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionWriteKind {
    /// Submit one order.
    Submit,
    /// Submit an order list.
    SubmitList,
    /// Modify one order.
    Modify,
    /// Modify multiple orders.
    BatchModify,
    /// Cancel one exact order.
    Cancel,
    /// Cancel multiple exact orders.
    BatchCancel,
    /// Cancel every order on one instrument, deny-only at the first-release boundary.
    CancelAll,
    /// Retry a previously constructed mutation.
    Retry,
}

/// Upstream identity bound to the exact command activated after persistence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExecutionWriteIdentity {
    /// Command identifier which must be reused for publication and transport.
    pub command_id: UUID4,
    /// Exact client order identifier affected by the command, when singular.
    pub client_order_id: Option<ClientOrderId>,
    /// Exact instrument affected by the command.
    pub instrument_id: InstrumentId,
    /// Native mutation kind.
    pub kind: ExecutionWriteKind,
    /// Deadline after which activation must fail.
    pub expires_at: UnixNanos,
}

/// Typed two-phase activation failure.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ExecutionWriteActivationError {
    /// No activator was installed for a staged command.
    #[error("persisted execution write activator is missing")]
    MissingActivator,
    /// The stage token was absent, expired, consumed, or did not match the identity.
    #[error("persisted execution write activation was rejected: {0}")]
    Rejected(String),
    /// The persistence receipt did not cover the exact current sequence.
    #[error("persisted execution write receipt was rejected: {0}")]
    Receipt(String),
}

/// Application-owned registry which converts an opaque stage into one publishable command.
pub trait PersistedExecutionWriteActivator: Debug + Send + Sync {
    /// Activates `identity` only when `stage` and `persistence` match its staged authorization.
    ///
    /// # Errors
    ///
    /// Returns an error for an absent, expired, consumed, mismatched, or insufficiently durable
    /// stage. A failure must leave the command unpublished.
    fn activate_persisted(
        &self,
        stage: ExecutionWriteStageToken,
        identity: ExecutionWriteIdentity,
        persistence: PersistenceReceipt,
    ) -> Result<(), ExecutionWriteActivationError>;

    /// Permanently halts application admission after a persistence-path failure.
    fn halt(&self, cause: &PersistenceError);

    /// Permanently halts application admission after stage activation fails.
    fn halt_activation(&self, cause: &ExecutionWriteActivationError);
}

/// Shared activator handle injected into a venue-capable node.
pub type PersistedExecutionWriteActivatorHandle = Arc<dyn PersistedExecutionWriteActivator>;
