import webbrowser
import json
import logging
import os
from PyQt5.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFrame, QLabel, 
    QTableWidget, QTableWidgetItem, QPushButton, QTextEdit, QSizePolicy
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QClipboard
from PyQt5.QtWidgets import QApplication

class AddressDetailsDialog(QDialog):
    def __init__(self, parent=None, address="", table_data=None, notes_file="", staker_data=None, all_addresses_data=None):
        super().__init__(parent)
        self.address = address
        self.table_data = table_data or {}
        self.notes_file = notes_file
        self.staker_data = staker_data or {}
        self.all_addresses_data = all_addresses_data or {}
        self.address_notes = self.load_address_notes()
        self.setup_ui()

    def load_address_notes(self):
        """Load saved address notes from JSON file"""
        try:
            if os.path.exists(self.notes_file):
                with open(self.notes_file, "r") as f:
                    return json.load(f)
        except Exception as e:
            logging.error(f"Error loading notes: {e}")
        return {}

    def save_address_notes(self):
        """Save notes for this address"""
        try:
            notes = self.notes_edit.toPlainText()
            self.address_notes[self.address] = notes
            
            with open(self.notes_file, "w") as f:
                json.dump(self.address_notes, f, indent=2)
                
            logging.info(f"Saved notes for address {self.address[:6]}...")
        except Exception as e:
            logging.error(f"Error saving notes: {e}")

    def setup_ui(self):
        self.setWindowTitle(f"Address Analytics - {self.address[:6]}...{self.address[-4:]}")
        self.setFixedSize(900, 900)
        
        layout = QVBoxLayout()
        
        # --- Address Section ---
        addr_frame = QFrame()
        addr_frame.setFrameShape(QFrame.StyledPanel)
        addr_layout = QVBoxLayout()
        
        # Address with copy button
        addr_header = QHBoxLayout()
        addr_header.addWidget(QLabel("<h3>Wallet Address</h3>"))
        
        copy_btn = QPushButton("📋 Copy")
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(self.address))
        addr_header.addWidget(copy_btn)
        addr_header.addStretch()
        addr_layout.addLayout(addr_header)
        
        addr_layout.addWidget(QLabel(f"<code>{self.address}</code>"))
        
        # Explorer links
        explorers = QHBoxLayout()
        explorers.addWidget(QLabel("<b>Explorers:</b>"))
        
        sites = [
            ("Arbitrum Sepolia", f"https://sepolia.arbiscan.io/address/{self.address}"),
            ("Etherscan", f"https://etherscan.io/address/{self.address}"),
            ("Debank", f"https://debank.com/profile/{self.address}"),
            ("Zapper", f"https://zapper.fi/account/{self.address}"),
        ]
        
        for name, url in sites:
            btn = QPushButton(name)
            btn.clicked.connect(lambda _, u=url: webbrowser.open(u))
            explorers.addWidget(btn)
        
        addr_layout.addLayout(explorers)
        addr_frame.setLayout(addr_layout)
        layout.addWidget(addr_frame)
        
        # --- Current Balances Section ---
        balances_frame = QFrame()
        balances_frame.setFrameShape(QFrame.StyledPanel)
        balances_layout = QVBoxLayout()
        balances_layout.addWidget(QLabel("<h3>Current Balances</h3>"))
        
        # Create horizontal layout for COR balances
        cor_balance_layout = QHBoxLayout()
        
        # Add COR balance items
        if self.table_data:
            cor_balance_layout.addWidget(QLabel(
                f"<b>COR:</b> {float(self.table_data.get('cortensor_balance', 0)):,.2f}",
                self
            ))
            cor_balance_layout.addWidget(QLabel(
                f"<b>Staked:</b> {float(self.table_data.get('staked_balance', 0)):,.2f}",
                self
            ))
            cor_balance_layout.addWidget(QLabel(
                f"<b>Daily Reward:</b> {float(self.table_data.get('daily_reward', 0)):,.2f}",
                self
            ))
            cor_balance_layout.addWidget(QLabel(
                f"<b>Claimable:</b> {float(self.table_data.get('claimable_rewards', 0)):,.2f}",
                self
            ))
        
        cor_balance_layout.addStretch()
        balances_layout.addLayout(cor_balance_layout)
        balances_frame.setLayout(balances_layout)
        layout.addWidget(balances_frame)
        
        # --- Staking Position Analysis ---
        if self.staker_data:
            position_frame = QFrame()
            position_frame.setFrameShape(QFrame.StyledPanel)
            position_layout = QVBoxLayout()
            
            # Calculate totals from all addresses
            total_combined = sum(
                float(data.get('cortensor_balance', 0)) + 
                float(data.get('staked_balance', 0)) + 
                float(data.get('claimable_rewards', 0))
                for data in self.all_addresses_data.values()
            )
            
            # Get current address staked amount
            current_staked = self.table_data.get("staked_balance", 0)
            
            # Prepare sorted staking data
            sorted_stakers = sorted(
                [(float(data["raw_balance"])/(10**18), float(data["staked_balance"])) 
                 for data in self.staker_data.values()],
                reverse=True
            )
            
            # Find positions
            staked_position = None
            combined_position = None
            
            for idx, (staked, _) in enumerate(sorted_stakers, 1):
                if staked_position is None and abs(staked - current_staked) < 0.01:
                    staked_position = idx
                if combined_position is None and staked <= total_combined:
                    combined_position = idx
                if staked_position is not None and combined_position is not None:
                    break
            
            # Display positions
            if staked_position:
                # Create horizontal layout for staked position info
                staked_position_layout = QHBoxLayout()
                staked_position_layout.addWidget(QLabel(
                    f"<b>This address is position #{staked_position}</b> in the staking rankings"
                ))
                
                if staked_position > 1:
                    needed = sorted_stakers[staked_position-2][0] - current_staked
                    staked_position_layout.addWidget(QLabel(
                        f"<b>(Need {needed:,.2f} more for #{staked_position-1})</b>"
                    ))
                
                staked_position_layout.addStretch()
                position_layout.addLayout(staked_position_layout)

            if combined_position:
                # Create horizontal layout for combined position info
                combined_position_layout = QHBoxLayout()
                combined_position_layout.addWidget(QLabel(
                    f"<b>Your combined COR is position #{combined_position}</b> (Total: {total_combined:,.2f} COR)"
                ))
    
                if combined_position > 1:
                    needed = sorted_stakers[combined_position-2][0] - total_combined
                    combined_position_layout.addWidget(QLabel(
                        f"<b>(Need {needed:,.2f} more for #{combined_position-1})</b>"
                    ))
                
                combined_position_layout.addStretch()
                position_layout.addLayout(combined_position_layout)
            
            # Top 200 table
            position_layout.addWidget(QLabel("<b>Top 200 Stakers:</b>"))
            
            top_table = QTableWidget()
            top_table.setColumnCount(2)
            top_table.setRowCount(min(200, len(sorted_stakers)))
            top_table.setHorizontalHeaderLabels(["Position", "Staked COR"])
            
            for i in range(top_table.rowCount()):
                _, staked = sorted_stakers[i]
                top_table.setItem(i, 0, QTableWidgetItem(str(i+1)))
                top_table.setItem(i, 1, QTableWidgetItem(f"{staked:,.2f}"))
            
            top_table.resizeColumnsToContents()
            position_layout.addWidget(top_table)
            
            position_frame.setLayout(position_layout)
            layout.addWidget(position_frame)
        
        # --- Notes Section ---
        notes_frame = QFrame()
        notes_frame.setFrameShape(QFrame.StyledPanel)
        notes_layout = QVBoxLayout()
        notes_layout.addWidget(QLabel("<h3>Address Notes</h3>"))
        
        self.notes_edit = QTextEdit()
        self.notes_edit.setPlainText(self.address_notes.get(self.address, ""))
        self.notes_edit.setPlaceholderText("Add private notes about this address...")
        
        # Set fixed height for notes section 
        self.notes_edit.setFixedHeight(60)
        self.notes_edit.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        
        notes_layout.addWidget(self.notes_edit)
        
        save_notes = QPushButton("💾 Save Notes")
        save_notes.clicked.connect(self.save_address_notes)
        notes_layout.addWidget(save_notes)
        
        notes_frame.setLayout(notes_layout)
        layout.addWidget(notes_frame)
        
        # --- Close Button ---
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.close)
        layout.addWidget(close_btn)
        
        self.setLayout(layout)