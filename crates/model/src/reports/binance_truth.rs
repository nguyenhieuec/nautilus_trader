// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Identity-correlated Binance safety-truth and private-stream reports.

use std::collections::BTreeMap;

use nautilus_core::{UUID4, UnixNanos};
use serde::{Deserialize, Serialize};

use crate::{
    identifiers::{AccountId, ClientId, ClientOrderId},
    reports::{OrderStatusReport, PositionStatusReport},
    types::AccountBalance,
};

/// Correlation identity for one strict execution truth request.
pub type ReportRequestId = UUID4;

/// Lifecycle state of an authenticated private execution stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PrivateStreamState {
    Reconciling,
    Ready,
    Stale,
    Failed,
}

/// Client-scoped health for one private execution stream generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrivateStreamHealth {
    pub client_id: ClientId,
    pub state: PrivateStreamState,
    pub generation: u64,
    pub authenticated: bool,
    pub subscribed: bool,
    pub last_heartbeat: Option<UnixNanos>,
    pub detail: Option<String>,
}

/// One exact-order result, including authoritative venue absence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ExactOrderQueryResult {
    Found(Box<OrderStatusReport>),
    NotFound,
}

/// Account and product-mode facts that must survive adapter conversion.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BinanceModeProof {
    Spot {
        can_trade: bool,
        account_type: String,
    },
    UsdM {
        can_trade: bool,
        dual_side_position: bool,
        multi_assets_margin: bool,
    },
}

/// Complete client-scoped response for one strict truth request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BinanceTruthReport {
    pub request_id: ReportRequestId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub mode: BinanceModeProof,
    pub balances: Vec<AccountBalance>,
    pub positions: Vec<PositionStatusReport>,
    pub open_orders: Vec<OrderStatusReport>,
    pub exact_orders: BTreeMap<ClientOrderId, ExactOrderQueryResult>,
    pub ts_received: UnixNanos,
}
