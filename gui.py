import os
import sys
import json
import logging
import requests
import webbrowser
import subprocess
import threading
from packaging import version
from datetime import datetime

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTableWidget, QTableWidgetItem, QLineEdit, QLabel,
    QTabWidget, QFrame, QMessageBox, QListWidget, QListWidgetItem,
    QPlainTextEdit, QAbstractItemView, QScrollArea
)
from PyQt5.QtCore import Qt, QTimer, QThread, QObject, pyqtSignal, pyqtSlot

from data_fetcher import (
    load_addresses_from_data_json as load_addresses,
    fetch_data_stream,
    get_current_prices,
)
from address_dialog import AddressDetailsDialog  # address details dialog window (existing) :contentReference[oaicite:1]{index=1}

# Version should match your GitHub release tags (e.g., "1.0.0")
__version__ = "2.2.1"

# --------------------------- ADDED: Version check ---------------------------
def _early_version_check():
    """
    Minimal, non-invasive version check executed at import time.
    Compares local __version__ to the latest GitHub release tag. If a newer
    version exists, it prints a warning to stderr and continues startup.

    Set STAKE_MONITOR_SKIP_VERSION_CHECK=1 to skip this check.
    """
    try:
        if os.environ.get("STAKE_MONITOR_SKIP_VERSION_CHECK", "").strip() == "1":
            return
        resp = requests.get(
            "https://api.github.com/repos/scerb/stake_monitor/releases/latest",
            timeout=5
        )
        resp.raise_for_status()
        latest = resp.json().get("tag_name", "").lstrip("v")
        if latest and version.parse(latest) > version.parse(__version__):
            sys.stderr.write(
                f"[stake_monitor] Warning: A newer version ({latest}) is available. "
                f"You're running {__version__}. "
                "See: https://github.com/scerb/stake_monitor/releases/latest\n"
            )
    except Exception:
        # Network/api issues shouldn't block the app from starting.
        pass

_early_version_check()
# ------------------------- END ADDED: Version check -------------------------

###############################################################################
# Logging & Files
###############################################################################

def setup_logging():
    log_path = get_data_path('app.log')
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Application started")

def get_data_path(filename):
    """Get the correct path for data files, works for both dev and packaged exe"""
    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, filename)

def load_miner_data():
    """Load miner data with proper initialization"""
    data_path = get_data_path("data.json")
    try:
        with open(data_path, "r") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            data = {}

        default_miner = {
            "eth_balance": 0.0,
            "eth_value_usd": 0.0,
            "cortensor_balance": 0.0,
            "staked_balance": 0.0,
            "cortensor_value_usd": 0.0,
            "time_staked_ago": "",
            "claimable_rewards": 0.0,
            "current_apr": 0.0,
            "rewards_value_usd": 0.0,
            "daily_reward": 0.0,
            "total_claimed": 0,
            "claim_count": 0,
            "last_claim_block": None,
            "last_scanned_block": 20926952,
            "claim_history": []
        }

        for address in list(data.keys()):
            data[address] = {**default_miner, **data.get(address, {})}

        return data
    except Exception as e:
        logging.error(f"Error loading miner data: {str(e)}")
        return {}

def load_claim_history():
    """Load claim history data from claim_history.json"""
    claim_history_path = get_data_path("claim_history.json")
    try:
        with open(claim_history_path, "r") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return {}
        return data
    except Exception as e:
        logging.error(f"Error loading claim history: {str(e)}")
        return {}

def save_miner_data(data):
    """Save miner data to JSON"""
    try:
        data_path = get_data_path("data.json")
        with open(data_path, "w") as f:
            json.dump(data, f, indent=4)
        logging.info("Miner data saved successfully")
    except Exception as e:
        logging.error(f"Failed to save miner data: {str(e)}")
        print("Failed to save miner data:", e)

def load_settings():
    """Load settings from settings.txt"""
    settings_path = get_data_path("settings.txt")
    defaults = {
        "history_save_interval": 60,  # repurposed as refresh interval (minutes)
        "last_block": 20926952
    }

    try:
        with open(settings_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and "=" in line:
                    key, value = line.split("=", 1)
                    try:
                        defaults[key] = int(value)
                    except ValueError:
                        logging.warning(f"Invalid setting value: {line}")
        return defaults
    except FileNotFoundError:
        save_settings(defaults)
        return defaults
    except Exception as e:
        logging.error(f"Error loading settings: {str(e)}")
        return defaults

def save_settings(settings):
    """Save settings to settings.txt"""
    try:
        settings_path = get_data_path("settings.txt")
        with open(settings_path, "w") as f:
            for key, value in settings.items():
                f.write(f"{key}={value}\n")
        logging.info("Settings saved successfully")
    except Exception as e:
        logging.error(f"Failed to save settings: {str(e)}")

###############################################################################
# Background fetch worker (balances) — keeps GUI responsive
###############################################################################

class DataFetchWorker(QObject):
    progress = pyqtSignal(str, dict)   # (address, data)
    finished = pyqtSignal(dict)        # full_stats
    error = pyqtSignal(str)

    def __init__(self, addresses, btc_price=None, max_workers=8):
        super().__init__()
        self.addresses = list(addresses)
        self.btc_price = btc_price
        self.max_workers = max_workers
        self._stopped = False

    def stop(self):
        self._stopped = True

    def _progress_cb(self, addr, data):
        if not self._stopped:
            self.progress.emit(addr, data)

    @pyqtSlot()
    def run(self):
        try:
            if not self.addresses:
                self.finished.emit({})
                return
            full = fetch_data_stream(
                self.addresses,
                callback=self._progress_cb,
                max_workers=self.max_workers
            )
            if not self._stopped:
                self.finished.emit(full)
        except Exception as e:
            self.error.emit(str(e))

###############################################################################
# Claim scan worker — runs claim_history.py in a subprocess (no UI blocking)
###############################################################################

class ClaimScanWorker(QObject):
    line = pyqtSignal(str)
    finished = pyqtSignal(int)   # return code
    error = pyqtSignal(str)

    @pyqtSlot()
    def run(self):
        try:
            script = get_data_path("claim_history.py")
            if not os.path.exists(script):
                self.error.emit("claim_history.py not found")
                self.finished.emit(1)
                return

            # Run as a separate process to avoid any chance of blocking the GUI thread
            proc = subprocess.Popen(
                [sys.executable, script],
                cwd=os.path.dirname(script),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            if proc.stdout:
                for line in proc.stdout:
                    self.line.emit(line.rstrip())

            rc = proc.wait()
            self.finished.emit(rc)
        except Exception as e:
            self.error.emit(str(e))
            self.finished.emit(1)

###############################################################################
# Main Window
###############################################################################

class CortensorDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        setup_logging()
        logging.info("Initializing CortensorDashboard")

        self.setWindowTitle(f"scerb_stake_monitor v{__version__}")
        self.setGeometry(100, 100, 1600, 600)

        # Data stores
        self.miner_data = load_miner_data()
        self.claim_history = load_claim_history()
        self.settings = load_settings()
        self.addresses = [addr for addr in self.miner_data.keys() if addr.startswith("0x")]

        # Price cache
        self.btc_price = 0.0
        self.eth_price = 0.0
        self.cor_price = 0.0
        self.current_apr = 0.0

        self.notes_file = get_data_path("address_notes.json")

        # UI
        self.tab_widget = QTabWidget()
        self.setCentralWidget(self.tab_widget)

        self.main_tab = QWidget()
        self.tab_widget.addTab(self.main_tab, "Dashboard")

        self.address_tab = QWidget()
        self.tab_widget.addTab(self.address_tab, "Add/Remove Address")

        # Sorting & worker state
        self.sort_order = Qt.AscendingOrder
        self.sort_column = -1
        self._loading = False
        self._thread = None
        self._worker = None

        # Claim scan state
        self.claim_scan_in_progress = False
        self._claim_thread = None
        self._claim_worker = None

        # Build tabs
        self.init_main_tab()
        self.init_address_tab()

        # Refresh timer (table balances)
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.refresh_data_async)
        self.timer.start(self.settings["history_save_interval"] * 60 * 1000)

        # Price timer
        self.price_timer = QTimer(self)
        self.price_timer.timeout.connect(self.update_prices)
        self.price_timer.start(30000)

        # Daily scripts timer (stake encrypt/position remain as before)
        self.script_timer = QTimer(self)
        self.script_timer.timeout.connect(self.run_daily_scripts)
        self.script_timer.start(24 * 60 * 60 * 1000)

        # Claim scan timer — every 24 hours
        self.claim_timer = QTimer(self)
        self.claim_timer.timeout.connect(self.scan_claim_history_async)
        self.claim_timer.start(24 * 60 * 60 * 1000)

        # Update checks
        QTimer.singleShot(5000, self.check_for_updates)
        self.update_timer = QTimer(self)
        self.update_timer.timeout.connect(self.check_for_updates)
        self.update_timer.start(24 * 60 * 60 * 1000)

        # Initial actions (do not block UI):
        self.refresh_data_async()                              # balances in background
        QTimer.singleShot(10000, self.scan_claim_history_async)  # claims in background after startup

    ###########################################################################
    # Main Tab (Dashboard)
    ###########################################################################

    def init_main_tab(self):
        layout = QVBoxLayout()

        # Price display
        price_layout = QHBoxLayout()

        self.btc_price_label = QLabel("BTC: $0.00")
        self.btc_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #f7931a;")

        self.eth_price_label = QLabel("ETH: $0.00")
        self.eth_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #3498db;")

        self.cor_price_label = QLabel("COR: $0.00")
        self.cor_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #27ae60;")

        self.apr_label = QLabel("APR: 0%")
        self.apr_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #9b59b6;")

        price_layout.addWidget(self.btc_price_label)
        price_layout.addWidget(self.eth_price_label)
        price_layout.addWidget(self.cor_price_label)
        price_layout.addWidget(self.apr_label)
        price_layout.addStretch()
        layout.addLayout(price_layout)

        # Separator
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        layout.addWidget(line)

        # Status row
        self.status_label = QLabel("")
        layout.addWidget(self.status_label)

        # Main table
        self.table = QTableWidget()
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)

        self.table.setColumnCount(12)
        self.table.setHorizontalHeaderLabels([
            "Address", "ETH", "COR", "Staked", "Daily Reward", "Claimable",
            "Total Claimed", "Claim Count", "Time Staked", "Reward Value", "$ETH", "$COR"
        ])

        self.min_column_widths = {
            0: 390, 1: 80, 2: 80, 3: 110, 4: 110, 5: 110,
            6: 110, 7: 90, 8: 120, 9: 120, 10: 80, 11: 80
        }
        for col, width in self.min_column_widths.items():
            self.table.setColumnWidth(col, width)

        self.table.horizontalHeader().sectionClicked.connect(self.handle_sort)
        self.table.cellClicked.connect(self.show_address_details)  # needs the method implemented
        layout.addWidget(self.table)

        # Controls
        btn_row = QHBoxLayout()
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh_data_async)

        self.claim_scan_btn = QPushButton("Scan Claims Now")
        self.claim_scan_btn.clicked.connect(self.scan_claim_history_async)

        btn_row.addWidget(self.refresh_btn)
        btn_row.addWidget(self.claim_scan_btn)
        btn_row.addStretch()
        layout.addLayout(btn_row)

        self.main_tab.setLayout(layout)

    ###########################################################################
    # Add/Remove Tab (scrollable + batch add)
    ###########################################################################

    def init_address_tab(self):
        outer_layout = QVBoxLayout()

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        inner = QWidget()
        inner_layout = QVBoxLayout(inner)

        inner_layout.addWidget(QLabel("Current Addresses:"))
        self.addr_list_widget = QListWidget()
        self.addr_list_widget.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.addr_list_widget.setUniformItemSizes(True)
        self._refresh_address_list_widget()
        inner_layout.addWidget(self.addr_list_widget)

        inner_layout.addWidget(QLabel("Add / Remove a Single Address:"))
        h1 = QHBoxLayout()
        self.addr_input = QLineEdit()
        self.addr_input.setPlaceholderText("Enter address (0x...)")
        add_btn = QPushButton("Add One")
        add_btn.clicked.connect(self.add_address)
        remove_btn = QPushButton("Remove Selected")
        remove_btn.clicked.connect(self.remove_selected_addresses)
        h1.addWidget(self.addr_input)
        h1.addWidget(add_btn)
        h1.addWidget(remove_btn)
        inner_layout.addLayout(h1)

        inner_layout.addWidget(QLabel("Add Many (one per line):"))
        self.addr_multi = QPlainTextEdit()
        self.addr_multi.setPlaceholderText("0xabc...\n0xdef...\n...")
        self.addr_multi.setFixedHeight(120)
        inner_layout.addWidget(self.addr_multi)

        h2 = QHBoxLayout()
        add_many_btn = QPushButton("Add Many")
        add_many_btn.clicked.connect(self.add_many)
        clear_many_btn = QPushButton("Clear Box")
        clear_many_btn.clicked.connect(self.addr_multi.clear)
        h2.addWidget(add_many_btn)
        h2.addWidget(clear_many_btn)
        h2.addStretch()
        inner_layout.addLayout(h2)

        inner_layout.addWidget(QLabel("Refresh Interval (minutes):"))
        h3 = QHBoxLayout()
        self.interval_input = QLineEdit()
        self.interval_input.setPlaceholderText("e.g. 60")
        interval_btn = QPushButton("Set Interval")
        interval_btn.clicked.connect(self.set_history_interval)
        h3.addWidget(self.interval_input)
        h3.addWidget(interval_btn)
        h3.addStretch()
        inner_layout.addLayout(h3)

        self.addr_status = QLabel("")
        inner_layout.addWidget(self.addr_status)

        scroll.setWidget(inner)
        outer_layout.addWidget(scroll)
        self.address_tab.setLayout(outer_layout)

    def _refresh_address_list_widget(self):
        self.addr_list_widget.clear()
        for addr in sorted([a for a in self.miner_data.keys() if a.startswith("0x")]):
            self.addr_list_widget.addItem(QListWidgetItem(addr))

    ###########################################################################
    # Non-blocking balance refresh (streaming)
    ###########################################################################

    def refresh_data_async(self):
        if self._loading:
            return

        self.addresses = [a for a in self.miner_data.keys() if a.startswith("0x")]
        if not self.addresses:
            self.populate_table()
            return

        self._loading = True
        self.status_label.setText("Loading balances…")
        self.refresh_btn.setEnabled(False)

        self._thread = QThread()
        self._worker = DataFetchWorker(self.addresses, btc_price=self.btc_price, max_workers=8)
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.progress.connect(self._on_fetch_progress)
        self._worker.finished.connect(self._on_fetch_finished)
        self._worker.error.connect(self._on_fetch_error)

        self._worker.finished.connect(self._thread.quit)
        self._worker.finished.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)

        self._thread.start()

    @pyqtSlot(str, dict)
    def _on_fetch_progress(self, addr, data):
        if addr not in self.miner_data:
            self.miner_data[addr] = {}
        self.miner_data[addr].update(data)

        row = self._row_for_address.get(addr) if hasattr(self, "_row_for_address") else None
        if row is not None:
            ch = self.claim_history.get(addr.lower(), {"total_claimed": 0.0, "claim_count": 0})
            row_data = [
                addr,
                f"{data['eth_balance']:.4f}",
                f"{data['cortensor_balance']:.4f}",
                f"{data['staked_balance']:.4f}",
                f"{data['daily_reward']:.4f}",
                f"{data['claimable_rewards']:.4f}",
                f"{ch.get('total_claimed', 0.0):.4f}",
                f"{ch.get('claim_count', 0)}",
                data.get("time_staked_ago", "N/A"),
                f"${data['rewards_value_usd']:.2f}",
                f"${data['eth_value_usd']:.2f}",
                f"${data['cortensor_value_usd']:.2f}",
            ]
            for col, value in enumerate(row_data):
                item = QTableWidgetItem(value if col != 0 else addr)
                item.setTextAlignment(Qt.AlignCenter)
                if col == 0:
                    item.setForeground(Qt.blue)
                    f = item.font()
                    f.setUnderline(True)
                    item.setFont(f)
                    item.setToolTip("Click for address details")
                self.table.setItem(row, col, item)

        apr_val = data.get("current_apr", 0.0)
        if apr_val:
            self.current_apr = apr_val
            self.apr_label.setText(f"<font color='#9b59b6'>APR: {self.current_apr:.0%}</font>")

        self._recompute_totals_live()

    @pyqtSlot(dict)
    def _on_fetch_finished(self, stats):
        try:
            if stats:
                for addr, data in stats.items():
                    self.miner_data.setdefault(addr, {}).update(data)

            save_miner_data(self.miner_data)
            self.apr_label.setText(f"<font color='#9b59b6'>APR: {self.current_apr:.0%}</font>")
            self.populate_table()
        finally:
            self._loading = False
            self.status_label.setText("Loaded.")
            self.refresh_btn.setEnabled(True)

    @pyqtSlot(str)
    def _on_fetch_error(self, err):
        logging.error(f"Background fetch failed: {err}")
        self._loading = False
        self.status_label.setText("Fetch error (see log)")
        self.refresh_btn.setEnabled(True)

    ###########################################################################
    # Claim scan orchestration (startup + every 24h, non-blocking)
    ###########################################################################

    def scan_claim_history_async(self):
        if self.claim_scan_in_progress:
            return
        self.claim_scan_in_progress = True
        self.claim_scan_btn.setEnabled(False)
        self.status_label.setText("Scanning claim history in background…")

        self._claim_thread = QThread()
        self._claim_worker = ClaimScanWorker()
        self._claim_worker.moveToThread(self._claim_thread)

        self._claim_thread.started.connect(self._claim_worker.run)
        self._claim_worker.line.connect(self._on_claim_output)
        self._claim_worker.finished.connect(self._on_claim_finished)
        self._claim_worker.error.connect(self._on_claim_error)

        self._claim_worker.finished.connect(self._claim_thread.quit)
        self._claim_worker.finished.connect(self._claim_worker.deleteLater)
        self._claim_thread.finished.connect(self._claim_thread.deleteLater)

        self._claim_thread.start()

    @pyqtSlot(str)
    def _on_claim_output(self, line):
        # keep it lightweight; last line only
        if line:
            self.status_label.setText(f"Claims: {line[-120:]}")

    @pyqtSlot(int)
    def _on_claim_finished(self, rc):
        # Reload claim_history.json and update table columns/totals
        self.claim_history = load_claim_history()
        self.populate_table()
        self.status_label.setText("Claim history updated." if rc == 0 else "Claim scan finished with warnings.")
        self.claim_scan_in_progress = False
        self.claim_scan_btn.setEnabled(True)

    @pyqtSlot(str)
    def _on_claim_error(self, msg):
        logging.error(f"Claim scan error: {msg}")
        self.status_label.setText(f"Claim scan error: {msg}")
        self.claim_scan_in_progress = False
        self.claim_scan_btn.setEnabled(True)

    ###########################################################################
    # Table helpers
    ###########################################################################

    def populate_table(self):
        addresses = [a for a in self.miner_data.keys() if a.startswith("0x")]
        row_count = len(addresses) + 1
        self.table.setRowCount(row_count)

        # Map row for streaming updates
        self._row_for_address = {addr: i for i, addr in enumerate(addresses)}

        totals = {
            "eth": 0.0, "cor": 0.0, "staked": 0.0, "eth_usd": 0.0,
            "cor_usd": 0.0, "daily_reward": 0.0, "claimable": 0.0,
            "claimed": 0.0, "claim_count": 0, "rewards_usd": 0.0
        }

        for row, address in enumerate(addresses):
            d = self.miner_data.get(address, {})
            ch = self.claim_history.get(address.lower(), {"total_claimed": 0.0, "claim_count": 0})

            row_data = [
                address,
                f"{d.get('eth_balance', 0.0):.4f}",
                f"{d.get('cortensor_balance', 0.0):.4f}",
                f"{d.get('staked_balance', 0.0):.4f}",
                f"{d.get('daily_reward', 0.0):.4f}",
                f"{d.get('claimable_rewards', 0.0):.4f}",
                f"{ch.get('total_claimed', 0.0):.4f}",
                f"{ch.get('claim_count', 0)}",
                d.get("time_staked_ago", "N/A"),
                f"${d.get('rewards_value_usd', 0.0):.2f}",
                f"${d.get('eth_value_usd', 0.0):.2f}",
                f"${d.get('cortensor_value_usd', 0.0):.2f}",
            ]

            for col, value in enumerate(row_data):
                item = QTableWidgetItem(value)
                item.setTextAlignment(Qt.AlignCenter)
                if col == 0:
                    item.setForeground(Qt.blue)
                    f = item.font()
                    f.setUnderline(True)
                    item.setFont(f)
                    item.setToolTip("Click for address details")
                self.table.setItem(row, col, item)

            totals["eth"] += d.get("eth_balance", 0.0)
            totals["cor"] += d.get("cortensor_balance", 0.0)
            totals["staked"] += d.get("staked_balance", 0.0)
            totals["eth_usd"] += d.get("eth_value_usd", 0.0)
            totals["cor_usd"] += d.get("cortensor_value_usd", 0.0)
            totals["daily_reward"] += d.get("daily_reward", 0.0)
            totals["claimable"] += d.get("claimable_rewards", 0.0)
            totals["claimed"] += ch.get("total_claimed", 0.0)
            totals["claim_count"] += ch.get("claim_count", 0)
            totals["rewards_usd"] += d.get("rewards_value_usd", 0.0)

        self._totals = totals
        self._render_totals_row()

        for col, width in self.min_column_widths.items():
            if self.table.columnWidth(col) < width:
                self.table.setColumnWidth(col, width)

    def _recompute_totals_live(self):
        # Recompute from miner_data + claim_history
        addresses = [a for a in self.miner_data.keys() if a.startswith("0x")]
        totals = {
            "eth": 0.0, "cor": 0.0, "staked": 0.0, "eth_usd": 0.0,
            "cor_usd": 0.0, "daily_reward": 0.0, "claimable": 0.0,
            "claimed": 0.0, "claim_count": 0, "rewards_usd": 0.0
        }
        for a in addresses:
            d = self.miner_data.get(a, {})
            ch_a = self.claim_history.get(a.lower(), {"total_claimed": 0.0, "claim_count": 0})
            totals["eth"] += d.get("eth_balance", 0.0)
            totals["cor"] += d.get("cortensor_balance", 0.0)
            totals["staked"] += d.get("staked_balance", 0.0)
            totals["eth_usd"] += d.get("eth_value_usd", 0.0)
            totals["cor_usd"] += d.get("cortensor_value_usd", 0.0)
            totals["daily_reward"] += d.get("daily_reward", 0.0)
            totals["claimable"] += d.get("claimable_rewards", 0.0)
            totals["claimed"] += ch_a.get("total_claimed", 0.0)
            totals["claim_count"] += ch_a.get("claim_count", 0)
            totals["rewards_usd"] += d.get("rewards_value_usd", 0.0)
        self._totals = totals
        self._render_totals_row()

    def _render_totals_row(self):
        total_row = self.table.rowCount() - 1
        total_data = [
            "TOTAL",
            f"{self._totals['eth']:.4f}",
            f"{self._totals['cor']:.4f}",
            f"{self._totals['staked']:.4f}",
            f"{self._totals['daily_reward']:.4f}",
            f"{self._totals['claimable']:.4f}",
            f"{self._totals['claimed']:.4f}",
            f"{self._totals['claim_count']}",
            "",
            f"${self._totals['rewards_usd']:.2f}",
            f"${self._totals['eth_usd']:.2f}",
            f"${self._totals['cor_usd']:.2f}",
        ]
        for col, value in enumerate(total_data):
            item = QTableWidgetItem(value)
            item.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(total_row, col, item)

    ###########################################################################
    # Sorting
    ###########################################################################

    def handle_sort(self, column):
        if column < 0 or column >= self.table.columnCount():
            return

        self.sort_column = column
        self.sort_order = Qt.DescendingOrder if self.sort_order == Qt.AscendingOrder else Qt.AscendingOrder

        row_count = self.table.rowCount()
        col_count = self.table.columnCount()
        data_rows = []

        for row in range(row_count - 1):  # exclude TOTAL
            row_data = [self.table.item(row, col).text() for col in range(col_count)]
            data_rows.append(row_data)

        def try_cast(val):
            try:
                return float(val.replace('$', '').replace(',', ''))
            except Exception:
                return val

        data_rows.sort(
            key=lambda x: try_cast(x[column]),
            reverse=self.sort_order == Qt.DescendingOrder
        )

        for i, row_data in enumerate(data_rows):
            for j, val in enumerate(row_data):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(i, j, item)

        total_row_data = [self.table.item(row_count - 1, col).text() for col in range(col_count)]
        for j, val in enumerate(total_row_data):
            item = QTableWidgetItem(val)
            item.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(row_count - 1, j, item)

    ###########################################################################
    # Address details (restored)
    ###########################################################################

    def show_address_details(self, row, col):
        """Show detailed popup for clicked address (Address column only)."""
        if col != 0:
            return

        address = self.table.item(row, 0).text()
        if address == "TOTAL":
            return

        # Load staker balances data (if present)
        try:
            with open(get_data_path("staker_balances.json"), "r") as f:
                staker_data = json.load(f)
        except Exception as e:
            logging.error(f"Error loading staker data: {e}")
            staker_data = {}

        # Prepare table data from visible cells (fast + resilient)
        table_data = {
            "eth_balance": float(self.table.item(row, 1).text()),
            "cortensor_balance": float(self.table.item(row, 2).text()),
            "staked_balance": float(self.table.item(row, 3).text()),
            "daily_reward": float(self.table.item(row, 4).text()),
            "claimable_rewards": float(self.table.item(row, 5).text()),
            "total_claimed": float(self.table.item(row, 6).text()),
            "claim_count": int(self.table.item(row, 7).text()),
            "time_staked_ago": self.table.item(row, 8).text(),
            "rewards_value_usd": float(self.table.item(row, 9).text().replace('$', '')),
            "eth_value_usd": float(self.table.item(row, 10).text().replace('$', '')),
            "cortensor_value_usd": float(self.table.item(row, 11).text().replace('$', ''))
        }

        # Gather combined data across all addresses (for ranking insights)
        all_addresses_data = {}
        for r in range(self.table.rowCount() - 1):  # Skip TOTAL
            addr = self.table.item(r, 0).text()
            if addr == "TOTAL":
                continue
            all_addresses_data[addr] = {
                "cortensor_balance": float(self.table.item(r, 2).text()),
                "staked_balance": float(self.table.item(r, 3).text()),
                "claimable_rewards": float(self.table.item(r, 5).text())
            }

        # Open the analytics dialog (existing module) :contentReference[oaicite:2]{index=2}
        dialog = AddressDetailsDialog(
            parent=self,
            address=address,
            table_data=table_data,
            notes_file=self.notes_file,
            staker_data=staker_data,
            all_addresses_data=all_addresses_data
        )
        dialog.exec_()

    ###########################################################################
    # Prices
    ###########################################################################

    def update_prices(self):
        try:
            eth_price, cor_price, btc_price = get_current_prices()
            self.btc_price = btc_price
            self.eth_price = eth_price
            self.cor_price = cor_price

            self.btc_price_label.setText(f"<font color='#f7931a'>BTC: ${btc_price:,.2f}</font>")
            self.eth_price_label.setText(f"<font color='#3498db'>ETH: ${eth_price:,.2f}</font>")
            self.cor_price_label.setText(f"<font color='#27ae60'>COR: ${cor_price:,.6f}</font>")
            self.apr_label.setText(f"<font color='#9b59b6'>APR: {self.current_apr:.0%}</font>")

            self.flash_price_background()
        except Exception as e:
            logging.error(f"Error updating prices: {str(e)}")
            self.btc_price_label.setText("<font color='#f7931a'>BTC: API Error</font>")
            self.eth_price_label.setText("<font color='#3498db'>ETH: API Error</font>")
            self.cor_price_label.setText("<font color='#27ae60'>COR: API Error</font>")
            self.apr_label.setText("<font color='#9b59b6'>APR: N/A</font>")

    def flash_price_background(self):
        self.btc_price_label.setStyleSheet("""
            font-weight: bold; font-size: 14px; color: #f7931a; background-color: #fff3e0;
        """)
        self.eth_price_label.setStyleSheet("""
            font-weight: bold; font-size: 14px; color: #3498db; background-color: #e3f2fd;
        """)
        self.cor_price_label.setStyleSheet("""
            font-weight: bold; font-size: 14px; color: #27ae60; background-color: #e8f5e9;
        """)
        self.apr_label.setStyleSheet("""
            font-weight: bold; font-size: 14px; color: #9b59b6; background-color: #f5eef8;
        """)
        QTimer.singleShot(2000, lambda: [
            self.btc_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #f7931a;"),
            self.eth_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #3498db;"),
            self.cor_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #27ae60;"),
            self.apr_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #9b59b6;")
        ])

    ###########################################################################
    # Address management
    ###########################################################################

    def add_address(self):
        addr = self.addr_input.text().strip()
        if not addr:
            self.addr_status.setText("Please enter an address")
            return
        if addr in self.miner_data:
            self.addr_status.setText("Address already exists")
            return

        self.miner_data[addr] = {
            "eth_balance": 0.0,
            "eth_value_usd": 0.0,
            "cortensor_balance": 0.0,
            "staked_balance": 0.0,
            "cortensor_value_usd": 0.0,
            "time_staked_ago": "",
            "claimable_rewards": 0.0,
            "current_apr": 0.0,
            "rewards_value_usd": 0.0,
            "daily_reward": 0.0,
            "total_claimed": 0,
            "claim_count": 0,
            "last_claim_block": None,
            "last_scanned_block": 20926952,
            "claim_history": []
        }
        save_miner_data(self.miner_data)

        if addr not in self.addresses:
            self.addresses.append(addr)
        self._refresh_address_list_widget()
        self.addr_status.setText(f"Added: {addr}")
        self.addr_input.clear()

        self.refresh_data_async()
        logging.info(f"Added new miner: {addr}")

    def add_many(self):
        text = self.addr_multi.toPlainText().strip()
        if not text:
            self.addr_status.setText("Nothing to add.")
            return

        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        added = 0
        for addr in lines:
            if addr in self.miner_data:
                continue
            self.miner_data[addr] = {
                "eth_balance": 0.0,
                "eth_value_usd": 0.0,
                "cortensor_balance": 0.0,
                "staked_balance": 0.0,
                "cortensor_value_usd": 0.0,
                "time_staked_ago": "",
                "claimable_rewards": 0.0,
                "current_apr": 0.0,
                "rewards_value_usd": 0.0,
                "daily_reward": 0.0,
                "total_claimed": 0,
                "claim_count": 0,
                "last_claim_block": None,
                "last_scanned_block": 20926952,
                "claim_history": []
            }
            if addr not in self.addresses:
                self.addresses.append(addr)
            added += 1

        save_miner_data(self.miner_data)
        self._refresh_address_list_widget()
        self.addr_status.setText(f"Added {added} addresses.")
        self.addr_multi.clear()
        self.refresh_data_async()

    def remove_selected_addresses(self):
        items = self.addr_list_widget.selectedItems()
        if not items:
            self.addr_status.setText("No address selected.")
            return

        removed = 0
        for it in items:
            addr = it.text()
            if addr in self.miner_data:
                del self.miner_data[addr]
                removed += 1
            if addr in self.addresses:
                self.addresses.remove(addr)

        save_miner_data(self.miner_data)
        self._refresh_address_list_widget()
        self.addr_status.setText(f"Removed {removed} addresses.")
        self.refresh_data_async()

    def set_history_interval(self):
        """Set the refresh interval in minutes"""
        try:
            mins = int(self.interval_input.text())
            self.settings["history_save_interval"] = mins
            save_settings(self.settings)
            self.timer.stop()
            self.timer.start(mins * 60 * 1000)
            self.addr_status.setText(f"Refresh interval set to {mins} minutes.")
            logging.info(f"Refresh interval set to {mins} minutes")
        except Exception as e:
            self.addr_status.setText("Failed to set interval.")
            logging.error(f"Failed to set refresh interval: {str(e)}")

    ###########################################################################
    # Daily scripts (unchanged behavior)
    ###########################################################################

    def run_daily_scripts(self):
        try:
            logging.info("Starting daily script execution")

            encrypt_script = get_data_path("stake_encrypt.py")
            if os.path.exists(encrypt_script):
                def run_encrypt():
                    subprocess.run([sys.executable, encrypt_script], check=True)
                    logging.info("Completed stake_encrypt.py")

                    from stake_position import run_in_thread
                    def position_callback():
                        logging.info("Completed stake_position.py")
                        QTimer.singleShot(1000, self.refresh_data_async)

                    run_in_thread(callback=position_callback)

                thread = threading.Thread(target=run_encrypt, daemon=True)
                thread.start()

            logging.info("Daily scripts started in background threads")
        except Exception as e:
            logging.error(f"Error running scripts: {e}")

    ###########################################################################
    # Update checker (unchanged behavior)
    ###########################################################################

    def check_for_updates(self):
        try:
            response = requests.get(
                "https://api.github.com/repos/scerb/stake_monitor/releases/latest",
                timeout=10
            )
            response.raise_for_status()
            latest_release = response.json()
            latest_version = latest_release['tag_name'].lstrip('v')

            if version.parse(latest_version) > version.parse(__version__):
                self.show_update_notification(latest_version, latest_release)
        except Exception as e:
            logging.error(f"Update check failed: {str(e)}")

    def show_update_notification(self, new_version, release):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle(f"Stake Monitor v{new_version} Available")
        msg.setText(f"<b>Version {new_version} is available!</b>")
        msg.setInformativeText(
            f"You're using v{__version__}\n\n"
            f"{release.get('body', 'Bug fixes and improvements')}\n\n"
            "Would you like to download the update?"
        )
        msg.addButton("Download", QMessageBox.AcceptRole)
        msg.addButton("Later", QMessageBox.RejectRole)
        msg.release_url = release['html_url']
        msg.buttonClicked.connect(lambda btn: self.handle_update_response(btn, msg))
        msg.exec_()

    def handle_update_response(self, button, message_box):
        if button.text() == "Download":
            webbrowser.open(message_box.release_url)

###############################################################################
# Entrypoint
###############################################################################

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CortensorDashboard()
    window.show()
    sys.exit(app.exec_())
