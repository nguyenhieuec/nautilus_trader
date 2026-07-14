// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Typed execution anomalies that must cross the engine boundary synchronously.

use std::{error::Error, fmt::Debug};

use nautilus_core::UnixNanos;
use nautilus_model::identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, TradeId};

/// A venue fill rejected because it exceeds the cached order quantity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RejectedOverfillV1 {
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub instrument_id: InstrumentId,
    pub client_order_id: ClientOrderId,
    pub trade_id: TradeId,
    pub order_quantity_raw: u128,
    pub prior_filled_raw: u128,
    pub rejected_last_quantity_raw: u128,
    pub observed_at: UnixNanos,
}

/// Typed reasons that a fill was not applied to the internal order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FillValidationErrorV1 {
    DuplicateFill,
    DuplicatePositionFill,
    MissingCachedOrigin(ClientOrderId),
    RejectedOverfill(Box<RejectedOverfillV1>),
}

impl std::fmt::Display for FillValidationErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicateFill => f.write_str("duplicate fill"),
            Self::DuplicatePositionFill => f.write_str("duplicate position fill"),
            Self::MissingCachedOrigin(client_order_id) => {
                write!(f, "missing cached client origin for {client_order_id}")
            }
            Self::RejectedOverfill(event) => write!(
                f,
                "rejected overfill for {} from {}",
                event.client_order_id, event.client_id
            ),
        }
    }
}

impl Error for FillValidationErrorV1 {}

/// Synchronous boundary used by applications to latch rejected exposure changes.
pub trait ExecutionAnomalySink: Debug + Send + Sync {
    fn record_rejected_overfill(&self, event: RejectedOverfillV1);
}
