# tx_analyzer_app.py
# Standalone GUI for Transactions & PnL (COR/ETH)
# - Adds Calls/sec control (default 2)
# - Adds Start Block control (default 20926952)
# - Uses a copyable error dialog instead of QMessageBox for failures
# - "Combine ‘All addresses’" option collapses all addresses to a single running series.

import os
import sys
import csv
import json
import traceback
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Any, Tuple

from PyQt5.QtCore import Qt, QThread, pyqtSignal, pyqtSlot, QDate, QUrl, QPoint
from PyQt5.QtGui import QDesktopServices, QFont, QKeySequence, QBrush, QColor
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QTableWidget, QTableWidgetItem, QFileDialog,
    QComboBox, QDateEdit, QLineEdit, QFrame, QDialog, QPlainTextEdit, QCheckBox,
    QMenu, QAction, QShortcut
)

# Local module
import tx_fetcher


# ---------- Copyable error dialog ----------

class CopyableTextDialog(QDialog):
    def __init__(self, title: str, text: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(820, 420)
        v = QVBoxLayout(self)
        self.text = QPlainTextEdit(self)
        self.text.setReadOnly(False)  # allow selection + copy
        self.text.setPlainText(text)
        v.addWidget(self.text)
        h = QHBoxLayout()
        self.copy_btn = QPushButton("Copy All")
        self.copy_btn.clicked.connect(self.copy_all)
        self.close_btn = QPushButton("Close")
        self.close_btn.clicked.connect(self.close)
        h.addWidget(self.copy_btn)
        h.addStretch()
        h.addWidget(self.close_btn)
        v.addLayout(h)

    def copy_all(self):
        self.text.selectAll()
        self.text.copy()


# ---------- Helpers to adapt tx_fetcher rows to GUI schema ----------

def _to_gui_rows_from_tx_fetcher(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Map the tx_fetcher 'final_rows' schema to the GUI schema expected by this app.

    tx_fetcher rows contain keys like:
      - 'date_utc', 'tx_hash', 'address', 'block', 'from', 'to',
      - ETH legs: 'eth_in', 'eth_out', 'gas_fee_eth', 'eth_usd', 'gas_fee_usd',
        plus 'eth_in_usd', 'eth_out_usd', 'net_usd'
      - COR legs: 'cor_in', 'cor_out'
      - classification: 'trade_type' (buy/sell/stake/unstake/eth_transfer/etc.)
      - taxes/prices: 'tax_cor_est', 'tax_usd_est', 'unit_price_usd'
      - running balances: 'eth_balance_after', 'cor_wallet_after', 'cor_staked_after', 'cor_owned_after'
    """
    out: List[Dict[str, Any]] = []

    for r in rows:
        # Basic fields
        date = r.get("date_utc") or (
            datetime.utcfromtimestamp(int(r.get("timestamp", 0))).strftime("%Y-%m-%d %H:%M:%S UTC")
            if r.get("timestamp") else ""
        )
        txh = r.get("tx_hash", "")
        addr = r.get("address", "")
        frm  = r.get("from", "")
        to   = r.get("to", "")
        blk  = r.get("block", "")

        # ETH legs
        eth_in      = float(r.get("eth_in", 0.0) or 0.0)
        eth_out     = float(r.get("eth_out", 0.0) or 0.0)
        gas_eth     = float(r.get("gas_fee_eth", 0.0) or 0.0)
        gas_usd     = float(r.get("gas_fee_usd", 0.0) or 0.0)
        eth_usd     = float(r.get("eth_usd", 0.0) or 0.0)
        eth_in_usd  = float(r.get("eth_in_usd", 0.0) or 0.0)
        eth_out_usd = float(r.get("eth_out_usd", 0.0) or 0.0)

        # COR legs
        cor_in   = float(r.get("cor_in", 0.0) or 0.0)
        cor_out  = float(r.get("cor_out", 0.0) or 0.0)
        trade    = (r.get("trade_type") or "").lower()

        # Determine row kind
        is_cor_row = (
            (cor_in != 0.0) or (cor_out != 0.0) or
            (trade in ("buy", "sell", "stake", "unstake",
                       "staking_reward", "node_reward",
                       "airdrop_or_other", "transfer", "internal_transfer"))
        )

        if is_cor_row:
            action = trade if trade else "transfer"
            gui_row = {
                "date": date,
                "tx_hash": txh,
                "address": addr,
                "kind": "COR_TRANSFER",
                "action": action,
                "eth_received": eth_in,
                "eth_spent": eth_out,
                "eth_price_usd": eth_usd,
                "eth_in_usd": eth_in_usd,
                "eth_out_usd": eth_out_usd,
                "gas_eth": gas_eth,
                "gas_usd": gas_usd,
                "cor_delta": (cor_in - cor_out),
                "tax_tokens": float(r.get("tax_cor_est", 0.0) or 0.0),
                "tax_usd": float(r.get("tax_usd_est", 0.0) or 0.0),
                "unit_price_usd": (r.get("unit_price_usd", None)
                                   if r.get("unit_price_usd") is not None else None),
                "from": frm,
                "to": to,
                "block": blk
            }
        else:
            # ETH-only movement
            net = eth_in - eth_out
            direction = "IN" if net >= 0 else "OUT"
            gui_row = {
                "date": date,
                "tx_hash": txh,
                "address": addr,
                "kind": "ETH_TRANSFER",
                "direction": direction,
                "eth_amount": abs(net),
                "eth_price_usd": eth_usd,
                "gas_eth": gas_eth,
                "gas_usd": gas_usd,
                "from": frm,
                "to": to,
                "block": blk
            }

        # Pass through running balance columns if present
        for k in ("eth_balance_after", "cor_wallet_after", "cor_staked_after", "cor_owned_after"):
            if k in r:
                gui_row[k] = r.get(k)

        out.append(gui_row)

    return out


def _compute_pnl_from_rows(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Compute a PnL summary using a simple average-cost method from the already-indexed rows.
    - Realized PnL on sells = proceeds - avg_cost * qty_sold (sell tax subtracted from realized PnL)
    - Taxes are accumulated separately (both buy & sell token taxes).
    - Fees (gas) are accumulated separately.
    - Market value uses the last known COR trade price (from the most recent buy/sell unit_price_usd).
    """
    # Sort rows to apply trades chronologically
    def _blknum(x):
        try:
            return int(x.get("block") or 0)
        except Exception:
            return 0
    rows_sorted = sorted(rows, key=lambda r: (_blknum(r), r.get("date",""), r.get("tx_hash","")))

    # Track latest per-address owned if present to report precise ending tokens
    latest_owned_by_addr: Dict[str, float] = {}

    position_tokens = 0.0         # tokens currently held in the selected view
    cost_basis_usd = 0.0          # dollar cost currently in the position (includes buy-side token tax as cost)
    realized_pnl_usd = 0.0
    fees_usd_total = 0.0
    taxes_usd_total = 0.0
    last_trade_price = None       # last unit price observed (buy or sell)

    for r in rows_sorted:
        # Running balances present? capture latest owned per wallet
        if r.get("address") and r.get("cor_owned_after") is not None:
            latest_owned_by_addr[r["address"]] = float(r.get("cor_owned_after") or 0.0)

        fees_usd_total += float(r.get("gas_usd", 0.0) or 0.0)

        if r.get("kind") != "COR_TRANSFER":
            continue

        action = (r.get("action") or "").lower()
        delta  = float(r.get("cor_delta", 0.0) or 0.0)
        tax_usd = float(r.get("tax_usd", 0.0) or 0.0)
        eth_in_usd  = float(r.get("eth_in_usd", 0.0) or 0.0)
        eth_out_usd = float(r.get("eth_out_usd", 0.0) or 0.0)
        up = r.get("unit_price_usd", None)
        if isinstance(up, (int, float)):
            last_trade_price = float(up)

        if action == "buy" and delta > 0:
            # Treat the buy-side token tax as part of cost basis; fees (gas) tracked separately.
            position_tokens += delta
            cost_basis_usd += (eth_out_usd + tax_usd)
            taxes_usd_total += tax_usd

        elif action == "sell" and delta < 0:
            qty = min(-delta, position_tokens)
            avg_cost = (cost_basis_usd / position_tokens) if position_tokens > 0 else 0.0
            # realized PnL excludes gas (reported separately); subtract sell-side token tax from realized PnL
            realized_pnl_usd += (eth_in_usd - qty * avg_cost) - tax_usd
            taxes_usd_total += tax_usd
            # reduce position & basis
            position_tokens -= qty
            cost_basis_usd -= qty * avg_cost

        elif action in ("staking_reward", "node_reward", "airdrop_or_other"):
            # Free tokens (zero cost) — increase position; basis unchanged
            position_tokens += delta

        elif action in ("stake", "unstake"):
            # Wallet<->staked move — no effect on owned tokens or basis
            pass

        elif action in ("internal_transfer", "transfer"):
            # Non-taxable move; in combined view these usually net to 0.
            # We *do not* mutate cost basis; to keep ending balance coherent,
            # rely on running balances if available; else adjust position by delta.
            if not latest_owned_by_addr:
                position_tokens += delta

        else:
            # Unknown: fall back conservatively — adjust tokens, not basis
            position_tokens += delta

    # Prefer authoritative ending tokens from running balances if available
    if latest_owned_by_addr:
        ending_tokens = sum(latest_owned_by_addr.values()) if len(latest_owned_by_addr) > 1 else list(latest_owned_by_addr.values())[0]
    else:
        ending_tokens = position_tokens

    avg_cost_usd = (cost_basis_usd / ending_tokens) if ending_tokens > 0 else 0.0
    market_price = last_trade_price if last_trade_price is not None else 0.0
    market_value_usd = ending_tokens * market_price
    unrealized_pnl_usd = market_value_usd - cost_basis_usd

    return {
        "ending_tokens": float(round(ending_tokens, 6)),
        "avg_cost_usd": float(round(avg_cost_usd, 6)),
        "market_value_usd": float(round(market_value_usd, 2)),
        "realized_pnl_usd": float(round(realized_pnl_usd, 2)),
        "unrealized_pnl_usd": float(round(unrealized_pnl_usd, 2)),
        "fees_usd_total": float(round(fees_usd_total, 2)),
        "tax_usd_total": float(round(taxes_usd_total, 2)),
    }


def _wrap_report_for_gui(rows: List[Dict[str, Any]], calls_per_second: float, fallback_start_block: int) -> Dict[str, Any]:
    """
    Build the dict the GUI expects:
    {
      "transactions_reported": [...mapped rows...],
      "start_block_enforced": <best guess>,
      "calls_per_second": <rps>,
      "pnl": {...}
    }
    """
    mapped = _to_gui_rows_from_tx_fetcher(rows)
    blocks = [int(r.get("block", 0) or 0) for r in rows if r.get("block") not in (None, "")]
    start_block_used = min(blocks) if blocks else fallback_start_block
    pnl = _compute_pnl_from_rows(mapped)
    return {
        "transactions_reported": mapped,
        "start_block_enforced": start_block_used,
        "calls_per_second": calls_per_second,
        "pnl": pnl
    }


# ---------- Background worker ----------

class IndexWorker(QThread):
    progressed = pyqtSignal(str)
    finished_ok = pyqtSignal(dict)
    failed = pyqtSignal(str)

    def __init__(self, addresses, from_date: datetime, to_date: datetime, rps: float, start_block: int):
        super().__init__()
        self.addresses = addresses
        self.from_date = from_date
        self.to_date = to_date
        self.rps = rps
        self.start_block = start_block

    def run(self):
        try:
            self.progressed.emit("Starting index…")
            # Call tx_fetcher and adapt its list of rows into the dict the GUI expects
            rows = tx_fetcher.index_transactions_for_addresses(
                self.addresses,
                self.from_date,
                self.to_date,
                start_block_override=self.start_block,
                rate_limit_rps=self.rps  # tx_fetcher expects 'rate_limit_rps' (not 'rps')
            )
            report = _wrap_report_for_gui(rows, self.rps, self.start_block)
            self.finished_ok.emit(report)
        except Exception as e:
            msg = f"{e}\n\nTraceback:\n{traceback.format_exc()}"
            self.failed.emit(msg)


# ---------- Main Window ----------

def _load_addresses():
    try:
        # Prefer your dashboard loader if present
        from data_fetcher import load_addresses_from_data_json
        addrs = load_addresses_from_data_json()
        return [a for a in addrs if isinstance(a, str) and a.lower().startswith("0x")]
    except Exception:
        # Fallback to any helper in tx_fetcher (if defined)
        try:
            return tx_fetcher.load_my_addresses()
        except Exception:
            return []


class TxAnalyzerWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cortensor — Transactions & PnL")
        self.setGeometry(120, 120, 1560, 860)

        self.addresses_all = [a.lower() for a in _load_addresses()]
        self.selected_addresses = list(self.addresses_all) if self.addresses_all else []

        self._result = None  # last result dict

        main = QWidget()
        self.setCentralWidget(main)
        v = QVBoxLayout(main)

        # --- Controls row ---
        ctrl = QHBoxLayout()

        ctrl.addWidget(QLabel("Addresses:"))
        self.addr_combo = QComboBox()
        self.addr_combo.addItem("All addresses")
        for a in self.addresses_all:
            self.addr_combo.addItem(a)
        self.addr_combo.currentIndexChanged.connect(self._addr_changed)
        ctrl.addWidget(self.addr_combo)

        # Combine toggle (enabled only for "All addresses")
        self.combine_chk = QCheckBox("Combine ‘All addresses’ into one running balance")
        self.combine_chk.setChecked(True)
        self.combine_chk.setEnabled(True if self.addr_combo.currentIndex() == 0 else False)
        ctrl.addWidget(self.combine_chk)

        ctrl.addWidget(QLabel("From:"))
        self.from_date = QDateEdit()
        self.from_date.setCalendarPopup(True)
        self.from_date.setDate(QDate(2023, 1, 1))
        ctrl.addWidget(self.from_date)

        ctrl.addWidget(QLabel("To:"))
        self.to_date = QDateEdit()
        self.to_date.setCalendarPopup(True)
        self.to_date.setDate(QDate.currentDate())
        ctrl.addWidget(self.to_date)

        ctrl.addWidget(QLabel("Etherscan API Key:"))
        self.key_edit = QLineEdit()
        self.key_edit.setPlaceholderText("Optional (env ETHERSCAN_API_KEY or keys.json also works)")
        envk = os.getenv("ETHERSCAN_API_KEY")
        if envk:
            self.key_edit.setText(envk)
        ctrl.addWidget(self.key_edit)

        ctrl.addWidget(QLabel("Calls/sec:"))
        self.rps_edit = QLineEdit()
        self.rps_edit.setFixedWidth(60)
        self.rps_edit.setText(os.getenv("ETHERSCAN_RPS", "2"))
        ctrl.addWidget(self.rps_edit)

        ctrl.addWidget(QLabel("Start block:"))
        self.start_block_edit = QLineEdit()
        self.start_block_edit.setFixedWidth(110)
        self.start_block_edit.setText(os.getenv("COR_START_BLOCK", "20926952"))
        ctrl.addWidget(self.start_block_edit)

        self.scan_btn = QPushButton("Scan")
        self.scan_btn.clicked.connect(self._scan)
        ctrl.addWidget(self.scan_btn)

        self.export_btn = QPushButton("Export CSV")
        self.export_btn.clicked.connect(self._export_csv)
        self.export_btn.setEnabled(False)
        ctrl.addWidget(self.export_btn)

        ctrl.addStretch()
        v.addLayout(ctrl)

        # Status
        self.status_lbl = QLabel("")
        v.addWidget(self.status_lbl)

        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Sunken)
        v.addWidget(sep)

        # Table
        self.table = QTableWidget()
        # +4 running-balance columns (always present; filled when balances are available)
        self.table.setColumnCount(21)
        self.table.setHorizontalHeaderLabels([
            "Date (UTC)", "Tx Hash", "Wallet", "Type", "Action/Dir",
            "ETH Δ", "ETH @USD", "Gas ETH", "Gas USD",
            "COR Δ", "Tax COR", "Tax USD", "Unit Price USD",
            "From", "To", "Block", "Kind",
            "ETH Bal", "COR Wallet Bal", "COR Staked Bal", "COR Owned Bal"
        ])
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)

        # NEW: enable selection & copying + context menu on the table
        self.table.setSelectionBehavior(QTableWidget.SelectItems)
        self.table.setSelectionMode(QTableWidget.ExtendedSelection)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_table_context_menu)
        self.table.cellDoubleClicked.connect(self._on_table_cell_double_clicked)
        self.copy_shortcut = QShortcut(QKeySequence.Copy, self.table)
        self.copy_shortcut.activated.connect(self._copy_selection_to_clipboard)

        v.addWidget(self.table)

        # Summary
        self.summary_lbl = QLabel("")
        self.summary_lbl.setStyleSheet("font-weight: bold; font-size: 14px;")
        v.addWidget(self.summary_lbl)

    # --- UI handlers ---

    def _addr_changed(self, idx: int):
        if idx <= 0:
            self.selected_addresses = list(self.addresses_all)
            self.combine_chk.setEnabled(True)
        else:
            self.selected_addresses = [self.addr_combo.currentText().lower()]
            self.combine_chk.setEnabled(False)

    def _scan(self):
        # Wire API key / env (tx_fetcher reads keys.json; env is optional here)
        ak = self.key_edit.text().strip()
        if ak:
            os.environ["ETHERSCAN_API_KEY"] = ak

        # Validate key presence (tx_fetcher helper reads keys.json)
        if not hasattr(tx_fetcher, "_read_api_key") or not tx_fetcher._read_api_key():
            self._show_error_dialog("API key", "No Etherscan API key set; please enter one or set ETHERSCAN_API_KEY.")
            return

        # Reads Calls/sec and Start block
        try:
            rps = float(self.rps_edit.text().strip() or "2")
        except Exception:
            rps = 2.0
        try:
            start_block = int(self.start_block_edit.text().strip() or "20926952")
        except Exception:
            start_block = 20926952

        f = self.from_date.date().toPyDate()
        t = self.to_date.date().toPyDate()
        fdt = datetime(f.year, f.month, f.day)
        tdt = datetime(t.year, t.month, t.day, 23, 59, 59)

        self.status_lbl.setText("Indexing… (first run may be slow; cache speeds up subsequent runs)")
        self.scan_btn.setEnabled(False)
        self.export_btn.setEnabled(False)

        self.worker = IndexWorker(self.selected_addresses, fdt, tdt, rps=rps, start_block=start_block)
        self.worker.progressed.connect(lambda s: self.status_lbl.setText(s))
        self.worker.finished_ok.connect(self._scan_done)
        self.worker.failed.connect(self._scan_failed)
        self.worker.start()

    @pyqtSlot(dict)
    def _scan_done(self, res: dict):
        self._result = res
        self.status_lbl.setText(
            f"Index complete. Start block {res.get('start_block_enforced')} | Calls/sec {res.get('calls_per_second')}"
        )

        rows = res.get("transactions_reported", []) or []

        # If "All addresses" + Combine toggle = ON, collapse + compute combined running balances
        if self.addr_combo.currentIndex() == 0 and self.combine_chk.isChecked() and len(self.selected_addresses) > 1:
            rows = self._combine_rows(rows)

        # Recompute PnL for the actually displayed set (single wallet or combined)
        pnl = _compute_pnl_from_rows(rows)

        self._populate_table(rows)
        self._update_summary(pnl, rows)
        self.scan_btn.setEnabled(True)
        self.export_btn.setEnabled(True)

    @pyqtSlot(str)
    def _scan_failed(self, msg: str):
        self.status_lbl.setText("Scan failed.")
        self._show_error_dialog("Scan failed", msg)
        self.scan_btn.setEnabled(True)
        self.export_btn.setEnabled(False)

    def _show_error_dialog(self, title: str, message: str):
        dlg = CopyableTextDialog(title, message, self)
        dlg.exec_()

    # --- Table & summary ---

    def _linkify_tx_item(self, tx_hash: str) -> QTableWidgetItem:
        item = QTableWidgetItem(tx_hash or "")
        item.setTextAlignment(Qt.AlignCenter)
        # Visual hint: link style
        f = QFont()
        f.setUnderline(True)
        item.setFont(f)
        item.setForeground(QBrush(QColor(0, 102, 204)))  # link-ish blue
        # Store target URL + tooltip
        url = f"https://etherscan.io/tx/{tx_hash}" if tx_hash else ""
        item.setData(Qt.UserRole, url)
        item.setToolTip("Double‑click to open on Etherscan")
        return item

    def _populate_table(self, rows: List[Dict[str, Any]]):
        self.table.setRowCount(len(rows))
        widths = [
            160, 290, 340, 100, 140, 120, 100, 90, 100, 120, 100, 100, 120, 260, 260, 90, 120,
            120, 140, 140, 140
        ]
        for c, w in enumerate(widths):
            try:
                self.table.setColumnWidth(c, w)
            except Exception:
                pass

        def _set(row: int, col: int, text: Any):
            item = QTableWidgetItem(str(text))
            item.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(row, col, item)

        for i, r in enumerate(rows):
            kind = r.get("kind")
            if kind == "COR_TRANSFER":
                action = r.get("action")
                eth_delta = (r.get("eth_received", 0.0) - r.get("eth_spent", 0.0))
                _set(i, 0, r.get("date", ""))
                # Tx hash as link
                self.table.setItem(i, 1, self._linkify_tx_item(r.get("tx_hash", "")))
                _set(i, 2, r.get("address", ""))
                _set(i, 3, "COR")
                _set(i, 4, action)
                _set(i, 5, f"{eth_delta:+.6f}")
                _set(i, 6, f"{r.get('eth_price_usd', 0.0):.6f}")
                _set(i, 7, f"{r.get('gas_eth', 0.0):.6f}")
                _set(i, 8, f"{r.get('gas_usd', 0.0):.6f}")
                _set(i, 9, f"{r.get('cor_delta', 0.0):+.6f}")
                _set(i,10, f"{r.get('tax_tokens', 0.0):.6f}")
                _set(i,11, f"{r.get('tax_usd', 0.0):.6f}")
                up = r.get("unit_price_usd", None)
                _set(i,12, (f"{up:.6f}" if up is not None else "—"))
                _set(i,13, r.get("from", ""))
                _set(i,14, r.get("to", ""))
                _set(i,15, str(r.get("block", "")))
                _set(i,16, "COR_TRANSFER")
            else:
                _set(i, 0, r.get("date", ""))
                # Tx hash as link (also for ETH-only rows)
                self.table.setItem(i, 1, self._linkify_tx_item(r.get("tx_hash", "")))
                _set(i, 2, r.get("address", ""))
                _set(i, 3, "ETH")
                _set(i, 4, r.get("direction", ""))
                _set(i, 5, f"{r.get('eth_amount', 0.0):+.6f}")
                _set(i, 6, f"{r.get('eth_price_usd', 0.0):.6f}")
                _set(i, 7, f"{r.get('gas_eth', 0.0):.6f}")
                _set(i, 8, f"{r.get('gas_usd', 0.0):.6f}")
                _set(i, 9, "—")
                _set(i,10, "—")
                _set(i,11, "—")
                _set(i,12, "—")
                _set(i,13, r.get("from", ""))
                _set(i,14, r.get("to", ""))
                _set(i,15, str(r.get("block", "")))
                _set(i,16, "ETH_TRANSFER")

            # Running balances (filled when present)
            self.table.setItem(i, 17, QTableWidgetItem(self._fmt_float(r.get("eth_balance_after"))))
            self.table.setItem(i, 18, QTableWidgetItem(self._fmt_float(r.get("cor_wallet_after"))))
            self.table.setItem(i, 19, QTableWidgetItem(self._fmt_float(r.get("cor_staked_after"))))
            self.table.setItem(i, 20, QTableWidgetItem(self._fmt_float(r.get("cor_owned_after"))))

            # Center align last four
            for c in (17, 18, 19, 20):
                itm = self.table.item(i, c)
                if itm:
                    itm.setTextAlignment(Qt.AlignCenter)

    def _update_summary(self, pnl: dict, rows: List[Dict[str, Any]]):
        if pnl:
            s = (
                f"Ending COR: {pnl.get('ending_tokens', 0.0):,.6f} | "
                f"Avg cost: ${pnl.get('avg_cost_usd', 0.0):,.6f} | "
                f"Market value: ${pnl.get('market_value_usd', 0.0):,.2f} | "
                f"Realized PnL: ${pnl.get('realized_pnl_usd', 0.0):,.2f} | "
                f"Unrealized PnL: ${pnl.get('unrealized_pnl_usd', 0.0):,.2f} | "
                f"Fees (gas): ${pnl.get('fees_usd_total', 0.0):,.2f} | "
                f"Taxes (imputed): ${pnl.get('tax_usd_total', 0.0):,.2f}"
            )
        else:
            s = "No PnL computed."

        # Footer: show combined ending balances; COR fields to 2 decimals (as requested earlier)
        if rows and rows[-1].get("eth_balance_after") is not None:
            s += (
                "  ||  Combined ending — "
                f"ETH: {rows[-1].get('eth_balance_after', 0.0):,.6f} | "
                f"COR wallet: {rows[-1].get('cor_wallet_after', 0.0):,.2f} | "
                f"COR staked: {rows[-1].get('cor_staked_after', 0.0):,.2f} | "
                f"COR owned: {rows[-1].get('cor_owned_after', 0.0):,.2f}"
            )

        self.summary_lbl.setText(s)

    # --- CSV export ---

    def _export_csv(self):
        if not self._result:
            return
        path, _ = QFileDialog.getSaveFileName(self, "Export CSV", "tx_history.csv", "CSV Files (*.csv)")
        if not path:
            return
        rows = self._result.get("transactions_reported", [])
        headers = [
            "date","tx_hash","address","kind","action_or_direction","eth_delta","eth_price_usd","gas_eth","gas_usd",
            "cor_delta","tax_tokens","tax_usd","unit_price_usd","from","to","block"
        ]
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(headers)
                for r in rows:
                    if r.get("kind") == "COR_TRANSFER":
                        eth_delta = (r.get("eth_received", 0.0) - r.get("eth_spent", 0.0))
                        w.writerow([
                            r.get("date",""), r.get("tx_hash",""), r.get("address",""),
                            r.get("kind",""), r.get("action",""),
                            f"{eth_delta:+.6f}", f"{r.get('eth_price_usd',0.0):.6f}",
                            f"{r.get('gas_eth',0.0):.6f}", f"{r.get('gas_usd',0.0):.6f}",
                            f"{r.get('cor_delta',0.0):+.6f}",
                            f"{r.get('tax_tokens',0.0):.6f}", f"{r.get('tax_usd',0.0):.6f}",
                            (f"{r.get('unit_price_usd',0.0):.6f}" if r.get("unit_price_usd") is not None else ""),
                            r.get("from",""), r.get("to",""), r.get("block",""),
                        ])
                    else:
                        w.writerow([
                            r.get("date",""), r.get("tx_hash",""), r.get("address",""),
                            r.get("kind",""), r.get("direction",""),
                            f"{r.get('eth_amount',0.0):+.6f}", f"{r.get('eth_price_usd',0.0):.6f}",
                            f"{r.get('gas_eth',0.0):.6f}", f"{r.get('gas_usd',0.0):.6f}",
                            "", "", "", "",
                            r.get("from",""), r.get("to",""), r.get("block",""),
                        ])
        except Exception as e:
            self._show_error_dialog("Export error", str(e))

    # --- Table copy & context menu helpers ---

    def _open_tx_from_item(self, item: QTableWidgetItem):
        if not item:
            return
        if item.column() != 1:
            return
        url = item.data(Qt.UserRole)
        if isinstance(url, str) and url:
            QDesktopServices.openUrl(QUrl(url))

    def _on_table_cell_double_clicked(self, row: int, col: int):
        if col == 1:
            self._open_tx_from_item(self.table.item(row, col))

    def _show_table_context_menu(self, pos: QPoint):
        item = self.table.itemAt(pos)
        menu = QMenu(self.table)

        act_copy_cell = QAction("Copy", self.table)
        act_copy_cell.triggered.connect(lambda: self._copy_cell(item))
        menu.addAction(act_copy_cell)

        act_copy_sel = QAction("Copy selection", self.table)
        act_copy_sel.triggered.connect(self._copy_selection_to_clipboard)
        menu.addAction(act_copy_sel)

        if item and item.column() == 1:
            menu.addSeparator()
            act_open = QAction("Open in Etherscan", self.table)
            act_open.triggered.connect(lambda: self._open_tx_from_item(item))
            menu.addAction(act_open)

        menu.exec_(self.table.viewport().mapToGlobal(pos))

    def _copy_cell(self, item: QTableWidgetItem):
        if not item:
            return
        QApplication.clipboard().setText(item.text())

    def _copy_selection_to_clipboard(self):
        ranges = self.table.selectedRanges()
        if not ranges:
            # Fallback to current cell
            it = self.table.currentItem()
            if it:
                QApplication.clipboard().setText(it.text())
            return
        # Build TSV grid over the union of all ranges
        parts = []
        # To keep a consistent block, copy each range as its own grid
        for rng in ranges:
            rows = []
            for r in range(rng.topRow(), rng.bottomRow() + 1):
                cols = []
                for c in range(rng.leftColumn(), rng.rightColumn() + 1):
                    it = self.table.item(r, c)
                    cols.append("" if it is None else it.text())
                rows.append("\t".join(cols))
            parts.append("\n".join(rows))
        QApplication.clipboard().setText("\n".join(parts))

    # --- Helpers ---

    @staticmethod
    def _fmt_float(x):
        return "" if x is None else f"{float(x):.6f}"

    @staticmethod
    def _eth_net_from_row(r: Dict[str, Any]) -> float:
        """
        Net ETH delta for a single row, *including gas as a cost*.
        Positive if ETH increases for our wallets; negative if decreases.
        """
        kind = r.get("kind")
        gas = float(r.get("gas_eth", 0.0) or 0.0)

        if kind == "COR_TRANSFER":
            received = float(r.get("eth_received", 0.0) or 0.0)
            spent    = float(r.get("eth_spent", 0.0) or 0.0)
            return (received - spent) - gas
        else:  # ETH_TRANSFER
            amt = float(r.get("eth_amount", 0.0) or 0.0)
            direction = (r.get("direction") or "").upper()
            signed = amt if direction == "IN" else -amt
            return signed - gas

    @staticmethod
    def _is_stake_action(action: str) -> Tuple[bool, bool]:
        """
        Return (is_stake, is_unstake) booleans for COR actions.
        """
        if not action:
            return False, False
        a = action.lower()
        return (a == "stake", a == "unstake")

    def _pick_counterparty(self, values: List[str], my_addrs: set) -> str:
        """Pick a readable 'from'/'to' for combined rows."""
        # Unique, normalized set
        s = { (v or "").lower() for v in values if isinstance(v, str) and v }
        if not s:
            return "—"
        # Prefer external counterparties
        externals = sorted([v for v in s if v not in my_addrs])
        if len(externals) == 1:
            return externals[0]
        if len(externals) > 1:
            return "multiple"
        # All are our own addresses
        return "internal" if len(s) > 1 else next(iter(s))

    def _combine_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Collapse per-address rows into a single per-tx view and compute
        running combined balances across ALL selected addresses.
        - cancels internal moves automatically (summing deltas by tx_hash)
        - ETH Bal incorporates gas
        - COR Wallet/Stake obey stake/unstake semantics; Owned=Wallet+Staked
        - Fills From/To using aggregated counterparties
        """
        my_addrs = {a.lower() for a in (self.selected_addresses or self.addresses_all)}

        # 1) Group rows by tx hash
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in rows:
            h = r.get("tx_hash") or r.get("hash") or ""
            if not h:
                # keep non-hash rows as-is (rare) by minting a unique key
                h = f"__nohash__{r.get('date','')}_{r.get('block','')}_{id(r)}"
            groups[h].append(r)

        combined: List[Dict[str, Any]] = []

        # 2) For each tx, sum across all our addresses
        for txh, grp in groups.items():
            # Basic shared fields (pick first non-empty sensible values)
            first = sorted(grp, key=lambda x: (int(x.get("block", 0) or 0), x.get("date",""), x.get("address","")))[0]
            date  = first.get("date", "")
            block = first.get("block", "")
            price = float(first.get("eth_price_usd", 0.0) or 0.0)

            # Totals
            gas_eth_total  = sum(float(x.get("gas_eth", 0.0) or 0.0) for x in grp)
            gas_usd_total  = sum(float(x.get("gas_usd", 0.0) or 0.0) for x in grp)
            tax_tok_total  = sum(float(x.get("tax_tokens", 0.0) or 0.0) for x in grp)
            tax_usd_total  = sum(float(x.get("tax_usd", 0.0) or 0.0) for x in grp)

            # Counterparties
            from_vals = [x.get("from", "") for x in grp]
            to_vals   = [x.get("to", "") for x in grp]
            from_pick = self._pick_counterparty(from_vals, my_addrs)
            to_pick   = self._pick_counterparty(to_vals, my_addrs)

            # Determine if this tx has COR leg(s)
            has_cor = any((x.get("kind") == "COR_TRANSFER") for x in grp)

            if has_cor:
                # COR deltas (these cancel if internal transfer between own addresses)
                cor_delta_total = sum(float(x.get("cor_delta", 0.0) or 0.0) for x in grp)
                # ETH legs from COR tx perspective:
                eth_spent_total    = sum(float(x.get("eth_spent", 0.0) or 0.0) for x in grp)
                eth_received_total = sum(float(x.get("eth_received", 0.0) or 0.0) for x in grp)
                # Unit price (if any row computed one)
                unit_price = None
                for x in grp:
                    if x.get("unit_price_usd") is not None:
                        unit_price = float(x.get("unit_price_usd"))
                        break
                # Preferred action label
                actions = [x.get("action","") for x in grp if x.get("kind") == "COR_TRANSFER"]
                pref = next((a for a in actions if a in ("buy","sell","stake","unstake","staking_reward","node_reward","airdrop_or_other")), actions[0] if actions else "")

                combined.append({
                    "date": date,
                    "tx_hash": txh,
                    "address": "ALL_COMBINED",
                    "kind": "COR_TRANSFER",
                    "action": pref or "transfer",
                    "eth_received": round(eth_received_total, 18),
                    "eth_spent": round(eth_spent_total, 18),
                    "eth_price_usd": price,
                    "gas_eth": round(gas_eth_total, 18),
                    "gas_usd": round(gas_usd_total, 2),
                    "cor_delta": round(cor_delta_total, 8),
                    "tax_tokens": round(tax_tok_total, 8),
                    "tax_usd": round(tax_usd_total, 2),
                    "unit_price_usd": unit_price,
                    "from": from_pick,
                    "to": to_pick,
                    "block": block
                })
            else:
                # Pure ETH transfer(s) — net ETH change across all our addresses
                eth_in = 0.0
                eth_out = 0.0
                for x in grp:
                    amt = float(x.get("eth_amount", 0.0) or 0.0)
                    direction = (x.get("direction") or "").upper()
                    if direction == "IN":
                        eth_in += amt
                    elif direction == "OUT":
                        eth_out += amt
                    else:
                        # If not labeled, fall back to sign of amt
                        eth_in += max(0.0, amt)
                        eth_out += max(0.0, -amt)

                net_amt = eth_in - eth_out  # before gas
                direction = "IN" if net_amt >= 0 else "OUT"

                combined.append({
                    "date": date,
                    "tx_hash": txh,
                    "address": "ALL_COMBINED",
                    "kind": "ETH_TRANSFER",
                    "direction": direction,
                    "eth_amount": round(abs(net_amt), 18),  # table shows signed separately
                    "eth_price_usd": price,
                    "gas_eth": round(gas_eth_total, 18),
                    "gas_usd": round(gas_usd_total, 2),
                    "from": from_pick,
                    "to": to_pick,
                    "block": block
                })

        # 3) Sort combined list in chain/time order
        def _blk(r):
            try:
                return int(r.get("block", 0) or 0)
            except Exception:
                return 0
        combined.sort(key=lambda r: (_blk(r), r.get("date",""), r.get("tx_hash","")))

        # 4) Compute running balances across the combined stream
        eth_bal = 0.0
        cor_wallet = 0.0
        cor_staked = 0.0

        for r in combined:
            kind = r.get("kind")
            if kind == "COR_TRANSFER":
                # ETH
                net_eth = (float(r.get("eth_received", 0.0) or 0.0)
                           - float(r.get("eth_spent", 0.0) or 0.0)
                           - float(r.get("gas_eth", 0.0) or 0.0))
                eth_bal += net_eth

                # COR (wallet/staked rules)
                delta = float(r.get("cor_delta", 0.0) or 0.0)
                action = r.get("action", "")
                is_stake, is_unstake = self._is_stake_action(action)
                if is_stake:
                    # wallet -> staked (delta is negative wallet move)
                    cor_wallet += delta
                    cor_staked += (-delta)
                elif is_unstake:
                    # staked -> wallet
                    cor_wallet += delta
                    cor_staked -= delta
                else:
                    # buy/sell/reward/airdrop/internal/transfer all change wallet directly
                    cor_wallet += delta
            else:
                # ETH transfer row
                net_eth = self._eth_net_from_row(r)
                eth_bal += net_eth
                # No COR changes

            r["eth_balance_after"] = round(eth_bal, 6)
            r["cor_wallet_after"]  = round(cor_wallet, 6)
            r["cor_staked_after"]  = round(cor_staked, 6)
            r["cor_owned_after"]   = round(cor_wallet + cor_staked, 6)

        return combined


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = TxAnalyzerWindow()
    win.show()
    sys.exit(app.exec_())
