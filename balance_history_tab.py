import os
import json
import sys
import datetime
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QCheckBox, QListWidget, QListWidgetItem
)
from PyQt5.QtCore import QTimer, Qt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure

class BalanceHistoryTab(QWidget):
    def __init__(self, data_file='data.json', history_file='balance_history.json'):
        super().__init__()
        self.data_file = self.get_data_path(data_file)
        self.history_file = self.get_data_path(history_file)
        self.tab_widget = None  # set externally from main

        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.title = QLabel("Balance History Graph")
        self.layout.addWidget(self.title)

        self.toggle_usd = QCheckBox("Show Token Amounts Instead of USD")
        self.toggle_usd.stateChanged.connect(self.plot_history)
        self.layout.addWidget(self.toggle_usd)

        self.address_list = QListWidget()
        self.address_list.setSelectionMode(QListWidget.NoSelection)
        self.address_list.itemChanged.connect(self.plot_history)
        self.layout.addWidget(self.address_list)

        self.figure = Figure(figsize=(10, 5))
        self.canvas = FigureCanvas(self.figure)
        self.layout.addWidget(self.canvas)

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.check_and_save_snapshot)
        self.timer.start(self.get_interval_ms())

        self.plot_history()

    def get_data_path(self, filename):
        """Get the correct path for data files, works for both dev and packaged exe"""
        if getattr(sys, 'frozen', False):
            # Running as compiled exe
            base_path = os.path.dirname(sys.executable)
        else:
            # Running in dev mode
            base_path = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base_path, filename)

    def get_interval_ms(self):
        """Get save interval from data.json with fallback"""
        try:
            with open(self.data_file, 'r') as f:
                data = json.load(f)
                hours = max(int(data.get("history_save_interval", 4)), 1)  # Minimum 1 hour
                return hours * 60 * 60 * 1000
        except Exception as e:
            print(f"Error reading interval: {e}")
            return 4 * 60 * 60 * 1000  # Default 4 hours

    def set_tab_widget(self, tab_widget):
        """Connect to main tab widget for change events"""
        self.tab_widget = tab_widget
        self.tab_widget.currentChanged.connect(self.handle_tab_changed)

    def handle_tab_changed(self, index):
        """Refresh when this tab becomes active"""
        if self.tab_widget.widget(index) == self:
            self.plot_history()

    def check_and_save_snapshot(self):
        """Save periodic balance snapshot if needed"""
        try:
            now = datetime.datetime.utcnow()
            interval_hours = int(self.get_interval_ms() / (60 * 60 * 1000))
            rounded_hour = (now.hour // interval_hours) * interval_hours
            rounded_time = now.replace(hour=rounded_hour, minute=0, second=0, microsecond=0)
            timestamp_str = rounded_time.strftime('%Y-%m-%d %H:%M')

            history = self.load_json(self.history_file) or {}
            if timestamp_str in history:
                return

            data = self.load_json(self.data_file)
            if not data:
                print("No data available for snapshot")
                return

            # Handle both old and new data.json formats
            addresses = data.get("addresses", [addr for addr in data if addr.startswith("0x")])

            snapshot = {
                '__timestamp__': datetime.datetime.now().isoformat(),
                '__interval_hours__': interval_hours
            }
            total_tokens = 0.0
            coin_price = 0.0

            for address in addresses:
                info = data.get(address, {})
                token_balance = float(info.get('cortensor_balance', 0.0)) + float(info.get('staked_balance', 0.0))
                snapshot[address] = token_balance
                total_tokens += token_balance
                
                # Calculate price if not set and we have valid data
                if coin_price == 0 and token_balance > 0:
                    usd_value = float(info.get('cortensor_value_usd', 0.0))
                    if usd_value > 0:
                        coin_price = usd_value / token_balance

            snapshot['__total__'] = total_tokens
            snapshot['__price__'] = coin_price if coin_price > 0 else None

            history[timestamp_str] = snapshot
            self.save_json(self.history_file, history)
            print(f"Saved balance snapshot for {timestamp_str}")
            self.plot_history()
        except Exception as e:
            print(f"Error saving snapshot: {e}")

    def plot_history(self):
        """Update the history plot with current data"""
        try:
            self.figure.clear()
            ax = self.figure.add_subplot(111)

            history = self.load_json(self.history_file)
            if not history:
                ax.set_title("No balance history available. Data will appear after first interval.")
                self.canvas.draw()
                return

            # Sort dates chronologically
            dates = sorted(
                [d for d in history.keys() if not d.startswith('__')],
                key=lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M')
            )

            address_data = {}
            use_tokens = self.toggle_usd.isChecked()

            # Process all historical data
            for date in dates:
                snapshot = history[date]
                price = snapshot.get("__price__")
                for address, value in snapshot.items():
                    if address.startswith('__'):
                        continue
                    
                    if use_tokens or price is None:
                        display_value = float(value)
                    else:
                        display_value = float(value) * float(price)
                    
                    if address not in address_data:
                        address_data[address] = []
                    address_data[address].append(display_value)

            # Initialize address list if empty
            if self.address_list.count() == 0:
                for addr in address_data.keys():
                    item = QListWidgetItem("Total" if addr == "__total__" else addr)
                    item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                    item.setCheckState(Qt.Checked)
                    self.address_list.addItem(item)

            # Get currently selected addresses
            shown_addresses = []
            for i in range(self.address_list.count()):
                item = self.address_list.item(i)
                if item.checkState() == Qt.Checked:
                    addr_text = item.text()
                    shown_addresses.append("__total__" if addr_text == "Total" else addr_text)

            # Plot selected addresses
            for address, values in address_data.items():
                if address in shown_addresses and len(values) == len(dates):
                    label = "Total" if address == "__total__" else address
                    if address == '__total__':
                        ax.plot(dates, values, label=label, linewidth=2, color='black')
                    else:
                        ax.plot(dates, values, label=label)

            # Format plot
            y_label = "Token Balance" if use_tokens else "USD Value"
            ax.set_title(f"Balance Over Time ({y_label})")
            ax.set_xlabel("Date/Time")
            ax.set_ylabel(y_label)
            
            # Rotate x-axis labels for better readability
            if len(dates) > 5:
                ax.set_xticks(range(0, len(dates), max(1, len(dates)//5)))
                ax.set_xticklabels([dates[i] for i in range(0, len(dates), max(1, len(dates)//5))], rotation=45)
            
            ax.legend(loc='upper left', fontsize='small', ncol=2)
            ax.grid(True)
            self.figure.tight_layout()
            self.canvas.draw()
        except Exception as e:
            print(f"Error plotting history: {e}")

    def load_json(self, path):
        """Safely load JSON data with error handling"""
        if not os.path.exists(path):
            return None
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading {path}: {e}")
            return None

    def save_json(self, path, data):
        """Safely save JSON data with error handling"""
        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            print(f"Error saving {path}: {e}")
            return False

# Required for PyInstaller to find this module when compiled
if __name__ == '__main__':
    import sys
    from PyQt5.QtWidgets import QApplication
    app = QApplication(sys.argv)
    window = BalanceHistoryTab()
    window.show()
    sys.exit(app.exec_())