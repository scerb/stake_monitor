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
    QTabWidget, QFrame, QMessageBox
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QClipboard
from data_fetcher import (
    load_addresses_from_data_json as load_addresses,
    fetch_data,
    get_current_prices
)
from balance_history_tab import BalanceHistoryTab
from address_dialog import AddressDetailsDialog

# Version should match your GitHub release tags (e.g., "1.0.0")
__version__ = "2.0.0"

def setup_logging():
    log_path = get_data_path('app.log')
    logging.basicConfig(
        filename=log_path,
        level=logging.DEBUG,
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

def save_addresses_only(address_list):
    try:
        data_path = get_data_path("data.json")
        logging.info(f"Saving addresses to {data_path}")
        try:
            with open(data_path, "r") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                data = {}
        except:
            data = {}

        data["addresses"] = address_list

        with open(data_path, "w") as f:
            json.dump(data, f, indent=4)
        logging.info("Addresses saved successfully")
    except Exception as e:
        logging.error(f"Failed to save address list: {str(e)}")
        print("Failed to save address list:", e)

def save_stats_data(stats):
    try:
        data_path = get_data_path("data.json")
        logging.info(f"Saving stats data to {data_path}")
        try:
            with open(data_path, "r") as f:
                existing_data = json.load(f)
        except:
            existing_data = {}

        if not isinstance(existing_data, dict):
            existing_data = {}

        existing_data.update(stats)

        with open(data_path, "w") as f:
            json.dump(existing_data, f, indent=4)
        logging.info("Stats data saved successfully")
    except Exception as e:
        logging.error(f"Failed to save stats: {str(e)}")
        print("Failed to save stats:", e)

class CortensorDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        setup_logging()
        logging.info("Initializing CortensorDashboard")
        
        self.setWindowTitle(f"scerb_stake_monitor v{__version__}")
        self.setGeometry(100, 100, 1350, 600)  # Increased width for new columns

        # Initialize price variables
        self.btc_price = 0.0
        self.eth_price = 0.0
        self.cor_price = 0.0
        self.current_apr = 0.0  # New APR variable

        # Initialize notes system
        self.notes_file = get_data_path("address_notes.json")

        self.tab_widget = QTabWidget()
        self.setCentralWidget(self.tab_widget)

        self.main_tab = QWidget()
        self.tab_widget.addTab(self.main_tab, "Dashboard")

        self.address_tab = QWidget()
        self.tab_widget.addTab(self.address_tab, "Add/Remove Address")

        self.history_tab = BalanceHistoryTab()
        self.tab_widget.addTab(self.history_tab, "Balance History")

        self.sort_order = Qt.AscendingOrder
        self.sort_column = -1

        self.init_main_tab()
        self.init_address_tab()

        # Setup timers
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.load_data)
        self.history_interval_minutes = self.load_interval_from_data_json()
        self.timer.start(self.history_interval_minutes * 60 * 1000)

        self.price_timer = QTimer(self)
        self.price_timer.timeout.connect(self.update_prices)
        self.price_timer.start(30000)  # Update prices every 30 seconds

        # Setup daily script timer
        self.script_timer = QTimer(self)
        self.script_timer.timeout.connect(self.run_daily_scripts)
        self.script_timer.start(24 * 60 * 60 * 1000)  # 24 hours

        # Check for updates after 5 seconds (to avoid blocking startup)
        QTimer.singleShot(5000, self.check_for_updates)
        
        # Also check periodically (e.g., every 24 hours)
        self.update_timer = QTimer(self)
        self.update_timer.timeout.connect(self.check_for_updates)
        self.update_timer.start(24 * 60 * 60 * 1000)  # 24 hours

        # Run scripts immediately on startup (in background)
        QTimer.singleShot(5000, self.run_daily_scripts)

        self.load_data()

    def show_address_details(self, row, col):
        """Show detailed popup for clicked address"""
        if col != 0:  # Only for address column
            return
        
        address = self.table.item(row, 0).text()
        if address == "TOTAL":
            return

        # Load staker balances data
        try:
            with open(get_data_path("staker_balances.json"), "r") as f:
                staker_data = json.load(f)
        except Exception as e:
            logging.error(f"Error loading staker data: {e}")
            staker_data = {}

        # Prepare table data
        table_data = {
            "eth_balance": float(self.table.item(row, 1).text()),
            "cortensor_balance": float(self.table.item(row, 2).text()),
            "staked_balance": float(self.table.item(row, 3).text()),
            "daily_reward": float(self.table.item(row, 4).text()),
            "claimable_rewards": float(self.table.item(row, 5).text()),
            "time_staked_ago": self.table.item(row, 6).text(),
            "rewards_value_usd": float(self.table.item(row, 7).text().replace('$', '')),
            "eth_value_usd": float(self.table.item(row, 8).text().replace('$', '')),
            "cortensor_value_usd": float(self.table.item(row, 9).text().replace('$', ''))
        }

        # Get all addresses data from the current table
        all_addresses_data = {}
        for r in range(self.table.rowCount() - 1):  # Skip TOTAL row
            addr = self.table.item(r, 0).text()
            if addr == "TOTAL":
                continue
            all_addresses_data[addr] = {
                "cortensor_balance": float(self.table.item(r, 2).text()),
                "staked_balance": float(self.table.item(r, 3).text()),
                "claimable_rewards": float(self.table.item(r, 5).text())
            }

        # Create and show dialog
        dialog = AddressDetailsDialog(
            parent=self,
            address=address,
            table_data=table_data,
            notes_file=self.notes_file,
            staker_data=staker_data,
            all_addresses_data=all_addresses_data
        )
        dialog.exec_()
        
    def run_daily_scripts(self):
        """Run the daily update scripts in sequence using threads"""
        try:
            logging.info("Starting daily script execution")
            
            # Run stake_encrypt.py first in a thread
            encrypt_script = get_data_path("stake_encrypt.py")
            if os.path.exists(encrypt_script):
                def run_encrypt():
                    subprocess.run([sys.executable, encrypt_script], check=True)
                    logging.info("Completed stake_encrypt.py")
                    
                    # Then run stake_position.py in another thread
                    from stake_position import run_in_thread
                    def position_callback():
                        logging.info("Completed stake_position.py")
                        # Refresh the data if needed
                        QTimer.singleShot(1000, self.load_data)
                    
                    run_in_thread(callback=position_callback)
                
                thread = threading.Thread(target=run_encrypt)
                thread.daemon = True
                thread.start()
            
            logging.info("Daily scripts started in background threads")
        except Exception as e:
            logging.error(f"Error running scripts: {e}")

    def check_for_updates(self):
        """Check GitHub for new releases and notify user if update is available"""
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
        """Show update dialog with your repo's information"""
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle(f"Stake Monitor v{new_version} Available")
        msg.setText(f"<b>Version {new_version} is available!</b>")
        msg.setInformativeText(
            f"You're using v{__version__}\n\n"
            f"{release.get('body', 'Bug fixes and improvements')}\n\n"
            "Would you like to download the update?"
        )
        
        # Add custom buttons
        msg.addButton("Download", QMessageBox.AcceptRole)
        msg.addButton("Later", QMessageBox.RejectRole)
        
        # Store the URL in the message box object
        msg.release_url = release['html_url']
        
        # Connect the button click signal
        msg.buttonClicked.connect(lambda btn: self.handle_update_response(btn, msg))
        msg.exec_()

    def handle_update_response(self, button, message_box):
        """Handle the user's response to the update notification"""
        if button.text() == "Download":
            webbrowser.open(message_box.release_url)
        # No action needed for "Later" button

    def init_main_tab(self):
        layout = QVBoxLayout()

        # Price display at the top
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

        # Separator line
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        layout.addWidget(line)

        # Main table
        self.table = QTableWidget()
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        
        # Updated column count and order with Time Staked
        self.table.setColumnCount(10)
        self.table.setHorizontalHeaderLabels([
            "Address", "ETH", "COR", "Staked", "Daily Reward", "Claimable", 
            "Time Staked", "Reward Value", "$ETH", "$COR"
        ])
        
        # Set minimum column widths (adjust these values as needed)
        self.min_column_widths = {
            0: 390,  # Address
            1: 80,   # ETH
            2: 80,   # COR
            3: 110,  # Staked
            4: 110,  # Daily Reward
            5: 110,  # Claimable
            6: 120,  # Time Staked
            7: 120,  # Reward Value
            8: 80,   # $ETH
            9: 80    # $COR
        }
        
        # Apply minimum widths
        for col, width in self.min_column_widths.items():
            self.table.setColumnWidth(col, width)
        
        self.table.horizontalHeader().sectionClicked.connect(self.handle_sort)
        self.table.cellClicked.connect(self.show_address_details)
        
        layout.addWidget(self.table)

        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self.load_data)
        layout.addWidget(refresh_btn)

        self.main_tab.setLayout(layout)

    def update_prices(self):
        """Fetch and display current prices directly from APIs"""
        try:
            eth_price, cor_price, btc_price = get_current_prices()
            
            # Update our price variables
            self.btc_price = btc_price
            self.eth_price = eth_price
            self.cor_price = cor_price
            
            # Update the display with HTML for persistent colors
            self.btc_price_label.setText(f"<font color='#f7931a'>BTC: ${btc_price:,.2f}</font>")
            self.eth_price_label.setText(f"<font color='#3498db'>ETH: ${eth_price:,.2f}</font>")
            self.cor_price_label.setText(f"<font color='#27ae60'>COR: ${cor_price:,.6f}</font>")
            self.apr_label.setText(f"<font color='#9b59b6'>APR: {self.current_apr:.0%}</font>")
            
            # Optional: Flash the background when updated
            self.flash_price_background()
            
        except Exception as e:
            logging.error(f"Error updating prices: {str(e)}")
            # Show error state with persistent colors
            self.btc_price_label.setText("<font color='#f7931a'>BTC: API Error</font>")
            self.eth_price_label.setText("<font color='#3498db'>ETH: API Error</font>")
            self.cor_price_label.setText("<font color='#27ae60'>COR: API Error</font>")
            self.apr_label.setText("<font color='#9b59b6'>APR: N/A</font>")

    def flash_price_background(self):
        """Visual feedback when prices update"""
        self.btc_price_label.setStyleSheet("""
            font-weight: bold; 
            font-size: 14px; 
            color: #f7931a;
            background-color: #fff3e0;
        """)
        self.eth_price_label.setStyleSheet("""
            font-weight: bold; 
            font-size: 14px; 
            color: #3498db;
            background-color: #e3f2fd;
        """)
        self.cor_price_label.setStyleSheet("""
            font-weight: bold;
            font-size: 14px;
            color: #27ae60;
            background-color: #e8f5e9;
        """)
        self.apr_label.setStyleSheet("""
            font-weight: bold;
            font-size: 14px;
            color: #9b59b6;
            background-color: #f5eef8;
        """)
        
        # Reset after 500ms
        QTimer.singleShot(500, lambda: [
            self.btc_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #f7931a;"),
            self.eth_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #3498db;"),
            self.cor_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #27ae60;"),
            self.apr_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #9b59b6;")
        ])

    def handle_sort(self, column):
        if column < 0 or column >= self.table.columnCount():
            return

        self.sort_column = column
        self.sort_order = Qt.DescendingOrder if self.sort_order == Qt.AscendingOrder else Qt.AscendingOrder

        # Extract all data rows except the last (TOTAL row)
        row_count = self.table.rowCount()
        col_count = self.table.columnCount()
        data_rows = []

        for row in range(row_count - 1):
            row_data = [self.table.item(row, col).text() for col in range(col_count)]
            data_rows.append(row_data)

        # Sort based on selected column (handle numeric sort if possible)
        def try_cast(val):
            try:
                return float(val.replace('$', '').replace(',', ''))
            except:
                return val

        data_rows.sort(
            key=lambda x: try_cast(x[column]),
            reverse=self.sort_order == Qt.DescendingOrder
        )

        # Re-populate the table with sorted rows + total row
        for i, row_data in enumerate(data_rows):
            for j, val in enumerate(row_data):
                item = QTableWidgetItem(val)
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(i, j, item)

        # Preserve TOTAL row
        total_row_data = [self.table.item(row_count - 1, col).text() for col in range(col_count)]
        for j, val in enumerate(total_row_data):
            item = QTableWidgetItem(val)
            item.setTextAlignment(Qt.AlignCenter)
            self.table.setItem(row_count - 1, j, item)

    def load_data(self):
        try:
            logging.info("Loading data...")
            self.addresses = load_addresses()
            if not self.addresses:
                logging.warning("No addresses found to load")
                return

            stats = fetch_data(self.addresses, self.btc_price)
            if not stats:
                logging.error("No data returned from fetch_data")
                return

            save_stats_data(stats)

            # Get APR from first address that has it
            self.current_apr = next((data.get('current_apr', 0) for data in stats.values() if 'current_apr' in data), 0)
            
            row_count = len(self.addresses) + 1
            self.table.setRowCount(row_count)

            total_eth = 0.0
            total_cort = 0.0
            total_staked = 0.0
            total_eth_usd = 0.0
            total_cort_usd = 0.0
            total_reward = 0.0
            total_claimable = 0.0
            total_rewards_value = 0.0

            for row, address in enumerate(self.addresses):
                data = stats.get(address, {
                    "eth_balance": 0.0,
                    "cortensor_balance": 0.0,
                    "staked_balance": 0.0,
                    "daily_reward": 0.0,
                    "claimable_rewards": 0.0,
                    "time_staked_ago": "N/A",
                    "rewards_value_usd": 0.0,
                    "eth_value_usd": 0.0,
                    "cortensor_value_usd": 0.0
                })

                row_data = [
                    address,
                    f"{data['eth_balance']:.4f}",
                    f"{data['cortensor_balance']:.4f}",
                    f"{data['staked_balance']:.4f}",
                    f"{data['daily_reward']:.4f}",
                    f"{data['claimable_rewards']:.4f}",
                    data["time_staked_ago"],
                    f"${data['rewards_value_usd']:.2f}",
                    f"${data['eth_value_usd']:.2f}",
                    f"${data['cortensor_value_usd']:.2f}",
                ]

                for col, value in enumerate(row_data):
                    item = QTableWidgetItem(value)
                    item.setTextAlignment(Qt.AlignCenter)
                    
                    if col == 0:  # Address column
                        item.setForeground(Qt.blue)
                        font = item.font()
                        font.setUnderline(True)
                        item.setFont(font)
                        item.setToolTip("Click for address details")
                    
                    self.table.setItem(row, col, item)

                total_eth += data["eth_balance"]
                total_cort += data["cortensor_balance"]
                total_staked += data["staked_balance"]
                total_eth_usd += data["eth_value_usd"]
                total_cort_usd += data["cortensor_value_usd"]
                total_reward += data["daily_reward"]
                total_claimable += data["claimable_rewards"]
                total_rewards_value += data["rewards_value_usd"]

            total_row = row_count - 1
            total_data = [
                "TOTAL",
                f"{total_eth:.4f}",
                f"{total_cort:.4f}",
                f"{total_staked:.4f}",
                f"{total_reward:.4f}",
                f"{total_claimable:.4f}",
                "",
                f"${total_rewards_value:.2f}",
                f"${total_eth_usd:.2f}",
                f"${total_cort_usd:.2f}",
            ]
            for col, value in enumerate(total_data):
                item = QTableWidgetItem(value)
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(total_row, col, item)

            # Ensure minimum column widths are maintained
            for col, width in self.min_column_widths.items():
                if self.table.columnWidth(col) < width:
                    self.table.setColumnWidth(col, width)

            logging.info("Data loaded successfully")
            
        except Exception as e:
            logging.error(f"Error in load_data: {str(e)}")
            print(f"Error loading data: {e}")

    def init_address_tab(self):
        layout = QVBoxLayout()
        self.addr_input = QLineEdit()
        self.addr_input.setPlaceholderText("Enter new address to add")

        self.interval_input = QLineEdit()
        self.interval_input.setPlaceholderText("Save history every N hours")

        add_btn = QPushButton("Add Address")
        add_btn.clicked.connect(self.add_address)

        remove_btn = QPushButton("Remove Address")
        remove_btn.clicked.connect(self.remove_address)

        save_btn = QPushButton("Save Addresses")
        save_btn.clicked.connect(self.save_address_list)

        interval_btn = QPushButton("Set Interval")
        interval_btn.clicked.connect(self.set_history_interval)

        self.addr_status = QLabel("")
        self.addr_list_display = QLabel("")
        self.addr_list_display.setWordWrap(True)

        layout.addWidget(QLabel("Current Addresses:"))
        layout.addWidget(self.addr_list_display)
        layout.addWidget(QLabel("Modify Address List:"))
        layout.addWidget(self.addr_input)

        hlayout = QHBoxLayout()
        hlayout.addWidget(add_btn)
        hlayout.addWidget(remove_btn)
        hlayout.addWidget(save_btn)

        layout.addLayout(hlayout)

        layout.addWidget(QLabel("Balance Save Interval:"))
        layout.addWidget(self.interval_input)
        layout.addWidget(interval_btn)

        layout.addWidget(self.addr_status)
        self.address_tab.setLayout(layout)

        self.addresses = load_addresses()
        self.update_address_display()

    def add_address(self):
        addr = self.addr_input.text().strip()
        if addr and addr not in self.addresses:
            self.addresses.append(addr)
            self.addr_status.setText(f"Added: {addr}")
            self.update_address_display()
            logging.info(f"Added address: {addr}")
        else:
            self.addr_status.setText("Invalid or duplicate address")
            logging.warning(f"Attempt to add invalid/duplicate address: {addr}")
        self.addr_input.clear()

    def remove_address(self):
        addr = self.addr_input.text().strip()
        if addr in self.addresses:
            self.addresses.remove(addr)
            self.addr_status.setText(f"Removed: {addr}")
            self.update_address_display()
            logging.info(f"Removed address: {addr}")
        else:
            self.addr_status.setText("Address not found")
            logging.warning(f"Attempt to remove non-existent address: {addr}")
        self.addr_input.clear()

    def save_address_list(self):
        save_addresses_only(self.addresses)
        self.addr_status.setText("Addresses saved")
        logging.info("Address list saved")
        self.load_data()

    def update_address_display(self):
        self.addr_list_display.setText("\n".join(self.addresses))

    def set_history_interval(self):
        try:
            hours = int(self.interval_input.text())
            data_path = get_data_path("data.json")
            
            try:
                with open(data_path, "r") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    data = {}
            except:
                data = {}

            data["history_save_interval"] = hours
            
            with open(data_path, "w") as f:
                json.dump(data, f, indent=4)
                
            self.timer.stop()
            self.timer.start(hours * 60 * 60 * 1000)
            self.addr_status.setText(f"Interval set to {hours} hours.")
            logging.info(f"History interval set to {hours} hours")
            
        except Exception as e:
            self.addr_status.setText("Failed to set interval.")
            logging.error(f"Failed to set history interval: {str(e)}")

    def load_interval_from_data_json(self):
        try:
            data_path = get_data_path("data.json")
            with open(data_path, "r") as f:
                data = json.load(f)
                interval = int(data.get("history_save_interval", 60))
                logging.info(f"Loaded history interval: {interval} minutes")
                return interval
        except Exception as e:
            logging.error(f"Error loading interval, using default: {str(e)}")
            return 60


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CortensorDashboard()
    window.show()
    sys.exit(app.exec_())