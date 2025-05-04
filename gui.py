import os
import sys
import json
import logging
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QTableWidget, QTableWidgetItem, QLineEdit, QLabel, 
    QTabWidget, QFrame
)
from PyQt5.QtCore import Qt, QTimer
from data_fetcher import get_current_prices

from data_fetcher import (
    load_addresses_from_data_json as load_addresses,
    fetch_data
)
from balance_history_tab import BalanceHistoryTab

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
        
        self.setWindowTitle("Cortensor Dashboard")
        self.setGeometry(100, 100, 1100, 600)

        # Initialize price variables
        self.eth_price = 0.0
        self.cor_price = 0.0

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

        self.load_data()

    def init_main_tab(self):
        layout = QVBoxLayout()

        # Price display at the top
        price_layout = QHBoxLayout()
        
        self.eth_price_label = QLabel("ETH: $0.00")
        self.eth_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #3498db;")
        price_layout.addWidget(self.eth_price_label)
        
        self.cor_price_label = QLabel("COR: $0.00")
        self.cor_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #27ae60;")
        price_layout.addWidget(self.cor_price_label)
        
        price_layout.addStretch()  # Push prices to the right
        
        layout.addLayout(price_layout)

        # Separator line
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        layout.addWidget(line)

        # Main table
        self.table = QTableWidget()
        self.table.setColumnCount(8)
        self.table.setHorizontalHeaderLabels([
            "Address", "ETH", "COR", "Staked", "Daily Reward", "Time Staked", "$ETH", "$COR"
        ])
        self.table.horizontalHeader().sectionClicked.connect(self.handle_sort)
        layout.addWidget(self.table)

        refresh_btn = QPushButton("Refresh")
        refresh_btn.clicked.connect(self.load_data)
        layout.addWidget(refresh_btn)

        self.main_tab.setLayout(layout)

    def update_prices(self):
        """Fetch and display current prices directly from APIs"""
        try:
            eth_price, cor_price = get_current_prices()
            
            # Update our price variables
            self.eth_price = eth_price
            self.cor_price = cor_price
            
            # Update the display
            self.eth_price_label.setText(f"ETH: ${eth_price:,.2f}")
            self.cor_price_label.setText(f"COR: ${cor_price:,.6f}")  # More decimals for COR
            
            # Optional: Flash the background when updated
            self.flash_price_background()
            
        except Exception as e:
            logging.error(f"Error updating prices: {str(e)}")
            # Show error state
            self.eth_price_label.setText("ETH: API Error")
            self.cor_price_label.setText("COR: API Error")

    def flash_price_background(self):
        """Visual feedback when prices update"""
        self.eth_price_label.setStyleSheet(
            "font-weight: bold; font-size: 14px; color: #3498db;"
            "background-color: #e3f2fd;"
        )
        self.cor_price_label.setStyleSheet(
            "font-weight: bold; font-size: 14px; color: #27ae60;"
            "background-color: #ffebee;"
        )
        
        # Reset after 500ms
        QTimer.singleShot(500, lambda: [
            self.eth_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #3498db;"),
            self.cor_price_label.setStyleSheet("font-weight: bold; font-size: 14px; color: #27ae60;")
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

            stats = fetch_data(self.addresses)
            if not stats:
                logging.error("No data returned from fetch_data")
                return

            save_stats_data(stats)

            row_count = len(self.addresses) + 1
            self.table.setRowCount(row_count)

            total_eth = 0.0
            total_cort = 0.0
            total_staked = 0.0
            total_eth_usd = 0.0
            total_cort_usd = 0.0
            total_reward = 0.0

            for row, address in enumerate(self.addresses):
                data = stats.get(address, {
                    "eth_balance": 0.0,
                    "cortensor_balance": 0.0,
                    "staked_balance": 0.0,
                    "eth_value_usd": 0.0,
                    "cortensor_value_usd": 0.0,
                    "time_staked_ago": "N/A"
                })
                staked = data["staked_balance"]
                daily_reward = staked * 0.0016438356164384

                row_data = [
                    address,
                    f"{data['eth_balance']:.4f}",
                    f"{data['cortensor_balance']:.4f}",
                    f"{staked:.4f}",
                    f"{daily_reward:.4f}",
                    data["time_staked_ago"],
                    f"${data['eth_value_usd']:.2f}",
                    f"${data['cortensor_value_usd']:.2f}",
                ]

                for col, value in enumerate(row_data):
                    item = QTableWidgetItem(value)
                    item.setTextAlignment(Qt.AlignCenter)
                    self.table.setItem(row, col, item)

                total_eth += data["eth_balance"]
                total_cort += data["cortensor_balance"]
                total_staked += staked
                total_eth_usd += data["eth_value_usd"]
                total_cort_usd += data["cortensor_value_usd"]
                total_reward += daily_reward

            total_row = row_count - 1
            total_data = [
                "TOTAL",
                f"{total_eth:.4f}",
                f"{total_cort:.4f}",
                f"{total_staked:.4f}",
                f"{total_reward:.4f}",
                "",
                f"${total_eth_usd:.2f}",
                f"${total_cort_usd:.2f}",
            ]
            for col, value in enumerate(total_data):
                item = QTableWidgetItem(value)
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(total_row, col, item)

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
