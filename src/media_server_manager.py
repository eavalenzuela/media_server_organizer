import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tkinter as tk
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from tkinter import colorchooser, filedialog, messagebox, simpledialog, ttk


DB_DEFAULT_PATH = os.path.join(os.path.expanduser("~"), ".media_server_organizer.db")

DEFAULT_THEME = {
    "window_background": "SystemButtonFace",
    "sidebar_background": "SystemWindow",
    "toolbar_background": "SystemButtonFace",
    "treeview_background": "SystemWindow",
    "metadata_background": "SystemButtonFace",
    "accent_color": "SystemHighlight",
    "text_color": "SystemWindowText",
}


@dataclass(frozen=True)
class Library:
    library_id: int
    name: str
    library_type: str
    path: str
    host: str | None
    username: str | None


class LibraryDB:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)
        self._init_schema()

    def _init_schema(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS libraries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                library_type TEXT NOT NULL,
                path TEXT NOT NULL,
                host TEXT,
                username TEXT
            )
            """
        )
        self.connection.commit()

    def fetch_libraries(self) -> list[Library]:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT id, name, library_type, path, host, username
            FROM libraries
            ORDER BY id
            """
        )
        return [
            Library(
                library_id=row[0],
                name=row[1],
                library_type=row[2],
                path=row[3],
                host=row[4],
                username=row[5],
            )
            for row in cursor.fetchall()
        ]

    def add_library(
        self, name: str, library_type: str, path: str, host: str | None, username: str | None
    ) -> Library:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO libraries (name, library_type, path, host, username)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, library_type, path, host, username),
        )
        self.connection.commit()
        return Library(
            library_id=cursor.lastrowid,
            name=name,
            library_type=library_type,
            path=path,
            host=host,
            username=username,
        )

    def update_library(
        self,
        library_id: int,
        name: str,
        library_type: str,
        path: str,
        host: str | None,
        username: str | None,
    ) -> Library:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            UPDATE libraries
            SET name = ?, library_type = ?, path = ?, host = ?, username = ?
            WHERE id = ?
            """,
            (name, library_type, path, host, username, library_id),
        )
        self.connection.commit()
        return Library(
            library_id=library_id,
            name=name,
            library_type=library_type,
            path=path,
            host=host,
            username=username,
        )

    def delete_library(self, library_id: int) -> None:
        cursor = self.connection.cursor()
        cursor.execute("DELETE FROM libraries WHERE id = ?", (library_id,))
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()


class NewLibraryDialog(tk.Toplevel):
    def __init__(self, master: tk.Tk) -> None:
        super().__init__(master)
        self.title("Add Library")
        self.resizable(False, False)
        self.result: dict[str, str | None] | None = None

        self.name_var = tk.StringVar()
        self.type_var = tk.StringVar(value="local")
        self.path_var = tk.StringVar()
        self.host_var = tk.StringVar()
        self.user_var = tk.StringVar()

        frame = ttk.Frame(self, padding=12)
        frame.grid(sticky="nsew")
        self.columnconfigure(0, weight=1)

        ttk.Label(frame, text="Name").grid(row=0, column=0, sticky="w")
        ttk.Entry(frame, textvariable=self.name_var, width=40).grid(row=0, column=1, sticky="ew")

        ttk.Label(frame, text="Type").grid(row=1, column=0, sticky="w", pady=(8, 0))
        type_frame = ttk.Frame(frame)
        type_frame.grid(row=1, column=1, sticky="w", pady=(8, 0))
        ttk.Radiobutton(type_frame, text="Local", variable=self.type_var, value="local").pack(
            side="left"
        )
        ttk.Radiobutton(type_frame, text="Remote (SSH)", variable=self.type_var, value="remote").pack(
            side="left", padx=(12, 0)
        )

        ttk.Label(frame, text="Path").grid(row=2, column=0, sticky="w", pady=(8, 0))
        path_frame = ttk.Frame(frame)
        path_frame.grid(row=2, column=1, sticky="ew", pady=(8, 0))
        path_entry = ttk.Entry(path_frame, textvariable=self.path_var, width=32)
        path_entry.pack(side="left", fill="x", expand=True)
        self.browse_button = ttk.Button(path_frame, text="Browse", command=self._browse)
        self.browse_button.pack(side="left", padx=(6, 0))

        ttk.Label(frame, text="Host").grid(row=3, column=0, sticky="w", pady=(8, 0))
        self.host_entry = ttk.Entry(frame, textvariable=self.host_var)
        self.host_entry.grid(row=3, column=1, sticky="ew", pady=(8, 0))

        ttk.Label(frame, text="Username").grid(row=4, column=0, sticky="w", pady=(8, 0))
        self.user_entry = ttk.Entry(frame, textvariable=self.user_var)
        self.user_entry.grid(row=4, column=1, sticky="ew", pady=(8, 0))

        button_frame = ttk.Frame(frame)
        button_frame.grid(row=5, column=0, columnspan=2, pady=(12, 0), sticky="e")
        ttk.Button(button_frame, text="Cancel", command=self._cancel).pack(side="right")
        ttk.Button(button_frame, text="Add", command=self._submit).pack(side="right", padx=(0, 8))

        frame.columnconfigure(1, weight=1)
        self.bind("<Return>", lambda event: self._submit())
        self.bind("<Escape>", lambda event: self._cancel())
        self.type_var.trace_add("write", lambda *_: self._toggle_remote_fields())
        self._toggle_remote_fields()
        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _browse(self) -> None:
        if self.type_var.get() == "local":
            folder = filedialog.askdirectory(parent=self)
            if folder:
                self.path_var.set(folder)

    def _toggle_remote_fields(self) -> None:
        is_remote = self.type_var.get() == "remote"
        state = "normal" if is_remote else "disabled"
        self.host_entry.configure(state=state)
        self.user_entry.configure(state=state)
        self.browse_button.configure(state="disabled" if is_remote else "normal")
        if not is_remote:
            self.host_var.set("")
            self.user_var.set("")

    def _submit(self) -> None:
        name = self.name_var.get().strip()
        library_type = self.type_var.get().strip()
        path = self.path_var.get().strip()
        host = self.host_var.get().strip() or None
        username = self.user_var.get().strip() or None

        if not name:
            messagebox.showerror("Missing name", "Please enter a library name.", parent=self)
            return
        if not path:
            messagebox.showerror("Missing path", "Please enter a library path.", parent=self)
            return
        if library_type == "remote" and not host:
            messagebox.showerror("Missing host", "Remote libraries require a host.", parent=self)
            return

        self.result = {
            "name": name,
            "library_type": library_type,
            "path": path,
            "host": host,
            "username": username,
        }
        self.destroy()

    def _cancel(self) -> None:
        self.destroy()


class LibraryManagementDialog(tk.Toplevel):
    def __init__(self, master: tk.Tk, db: LibraryDB) -> None:
        super().__init__(master)
        self.title("Library Management")
        self.resizable(False, False)
        self.db = db
        self.selected_library_id: int | None = None

        self.name_var = tk.StringVar()
        self.type_var = tk.StringVar(value="local")
        self.path_var = tk.StringVar()
        self.host_var = tk.StringVar()
        self.user_var = tk.StringVar()

        container = ttk.Frame(self, padding=12)
        container.grid(sticky="nsew")
        container.columnconfigure(1, weight=1)

        self.library_list = ttk.Treeview(
            container, columns=("name", "type", "path"), show="headings", height=8
        )
        self.library_list.heading("name", text="Name")
        self.library_list.heading("type", text="Type")
        self.library_list.heading("path", text="Path")
        self.library_list.column("name", width=140, anchor="w")
        self.library_list.column("type", width=80, anchor="w")
        self.library_list.column("path", width=230, anchor="w")
        self.library_list.grid(row=0, column=0, rowspan=6, sticky="nsw", padx=(0, 12))
        self.library_list.bind("<<TreeviewSelect>>", self._on_library_selected)

        ttk.Label(container, text="Name").grid(row=0, column=1, sticky="w")
        ttk.Entry(container, textvariable=self.name_var, width=36).grid(
            row=0, column=2, sticky="ew"
        )

        ttk.Label(container, text="Type").grid(row=1, column=1, sticky="w", pady=(6, 0))
        type_frame = ttk.Frame(container)
        type_frame.grid(row=1, column=2, sticky="w", pady=(6, 0))
        ttk.Radiobutton(type_frame, text="Local", variable=self.type_var, value="local").pack(
            side="left"
        )
        ttk.Radiobutton(type_frame, text="Remote", variable=self.type_var, value="remote").pack(
            side="left", padx=(12, 0)
        )

        ttk.Label(container, text="Path").grid(row=2, column=1, sticky="w", pady=(6, 0))
        path_frame = ttk.Frame(container)
        path_frame.grid(row=2, column=2, sticky="ew", pady=(6, 0))
        ttk.Entry(path_frame, textvariable=self.path_var, width=28).pack(
            side="left", fill="x", expand=True
        )
        self.browse_button = ttk.Button(path_frame, text="Browse", command=self._browse)
        self.browse_button.pack(side="left", padx=(6, 0))

        ttk.Label(container, text="Host").grid(row=3, column=1, sticky="w", pady=(6, 0))
        self.host_entry = ttk.Entry(container, textvariable=self.host_var, width=28)
        self.host_entry.grid(row=3, column=2, sticky="ew", pady=(6, 0))

        ttk.Label(container, text="Username").grid(row=4, column=1, sticky="w", pady=(6, 0))
        self.user_entry = ttk.Entry(container, textvariable=self.user_var, width=28)
        self.user_entry.grid(row=4, column=2, sticky="ew", pady=(6, 0))

        button_frame = ttk.Frame(container)
        button_frame.grid(row=5, column=1, columnspan=2, sticky="e", pady=(10, 0))
        ttk.Button(button_frame, text="Add Library", command=self._add_library).pack(
            side="left"
        )
        ttk.Button(button_frame, text="Remove", command=self._remove_library).pack(
            side="right", padx=(6, 0)
        )
        ttk.Button(button_frame, text="Save Changes", command=self._save_library).pack(
            side="right"
        )

        container.columnconfigure(2, weight=1)
        self.type_var.trace_add("write", lambda *_: self._toggle_remote_fields())
        self._toggle_remote_fields()
        self._load_libraries()
        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _load_libraries(self) -> None:
        for item in self.library_list.get_children():
            self.library_list.delete(item)
        for library in self.db.fetch_libraries():
            self.library_list.insert(
                "",
                "end",
                iid=str(library.library_id),
                values=(library.name, library.library_type, library.path),
            )
        if self.library_list.get_children():
            self.library_list.selection_set(self.library_list.get_children()[0])
            self._on_library_selected()

    def _on_library_selected(self, _event: tk.Event | None = None) -> None:
        selection = self.library_list.selection()
        if not selection:
            return
        library_id = int(selection[0])
        library = next(
            (item for item in self.db.fetch_libraries() if item.library_id == library_id), None
        )
        if not library:
            return
        self.selected_library_id = library.library_id
        self.name_var.set(library.name)
        self.type_var.set(library.library_type)
        self.path_var.set(library.path)
        self.host_var.set(library.host or "")
        self.user_var.set(library.username or "")

    def _browse(self) -> None:
        if self.type_var.get() == "local":
            folder = filedialog.askdirectory(parent=self)
            if folder:
                self.path_var.set(folder)

    def _toggle_remote_fields(self) -> None:
        is_remote = self.type_var.get() == "remote"
        state = "normal" if is_remote else "disabled"
        self.host_entry.configure(state=state)
        self.user_entry.configure(state=state)
        self.browse_button.configure(state="disabled" if is_remote else "normal")
        if not is_remote:
            self.host_var.set("")
            self.user_var.set("")

    def _validate_form(self) -> bool:
        name = self.name_var.get().strip()
        path = self.path_var.get().strip()
        if not name:
            messagebox.showerror("Missing name", "Please enter a library name.", parent=self)
            return False
        if not path:
            messagebox.showerror("Missing path", "Please enter a library path.", parent=self)
            return False
        if self.type_var.get() == "remote" and not self.host_var.get().strip():
            messagebox.showerror("Missing host", "Remote libraries require a host.", parent=self)
            return False
        return True

    def _add_library(self) -> None:
        dialog = NewLibraryDialog(self)
        self.wait_window(dialog)
        if not dialog.result:
            return
        self.db.add_library(
            name=dialog.result["name"],
            library_type=dialog.result["library_type"],
            path=dialog.result["path"],
            host=dialog.result["host"],
            username=dialog.result["username"],
        )
        self._load_libraries()

    def _save_library(self) -> None:
        if self.selected_library_id is None:
            messagebox.showinfo("Library Management", "Select a library to edit.", parent=self)
            return
        if not self._validate_form():
            return
        self.db.update_library(
            library_id=self.selected_library_id,
            name=self.name_var.get().strip(),
            library_type=self.type_var.get().strip(),
            path=self.path_var.get().strip(),
            host=self.host_var.get().strip() or None,
            username=self.user_var.get().strip() or None,
        )
        self._load_libraries()
        self.library_list.selection_set(str(self.selected_library_id))
        self._on_library_selected()

    def _remove_library(self) -> None:
        if self.selected_library_id is None:
            messagebox.showinfo("Library Management", "Select a library to remove.", parent=self)
            return
        if not messagebox.askyesno(
            "Remove Library",
            "Are you sure you want to remove this library?",
            parent=self,
        ):
            return
        self.db.delete_library(self.selected_library_id)
        self.selected_library_id = None
        self._load_libraries()


class ThemeEditorDialog(tk.Toplevel):
    COMPONENTS = [
        ("window_background", "Window Background"),
        ("sidebar_background", "Sidebar Background"),
        ("toolbar_background", "Toolbar Background"),
        ("treeview_background", "Library Tree Background"),
        ("metadata_background", "Metadata Panel Background"),
        ("accent_color", "Accent Color"),
        ("text_color", "Primary Text"),
    ]

    def __init__(
        self,
        master: tk.Tk,
        themes: dict[str, dict[str, str]],
        apply_theme_callback: Callable[[dict[str, str]], None] | None = None,
    ) -> None:
        super().__init__(master)
        self.title("Theme Editor")
        self.geometry("940x520")
        self.resizable(False, False)

        self.themes = themes
        self.theme_names = list(self.themes.keys())
        self.selected_theme_name = tk.StringVar(value=self.theme_names[0])
        self.selected_component: str | None = None
        self.apply_theme_callback = apply_theme_callback

        self.current_color = tk.StringVar(value="#3b74ff")
        self.hex_entry = tk.StringVar()
        self.picker_mode = tk.StringVar(value="screen")

        container = ttk.Frame(self, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(1, weight=1)
        container.rowconfigure(0, weight=1)

        self._build_theme_list(container)
        self._build_editor_panel(container)
        self._load_theme(self.theme_names[0])

        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _build_theme_list(self, parent: ttk.Frame) -> None:
        left_frame = ttk.Frame(parent)
        left_frame.grid(row=0, column=0, sticky="ns")

        ttk.Label(left_frame, text="Themes", font=("Segoe UI", 11, "bold")).pack(
            anchor="w"
        )
        list_frame = ttk.Frame(left_frame)
        list_frame.pack(fill="y", pady=(8, 0))

        self.theme_listbox = tk.Listbox(list_frame, height=16, width=22)
        scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.theme_listbox.yview)
        self.theme_listbox.configure(yscrollcommand=scroll.set)
        self.theme_listbox.pack(side="left", fill="y")
        scroll.pack(side="right", fill="y")

        for name in self.theme_names:
            self.theme_listbox.insert("end", name)
        self.theme_listbox.selection_set(0)
        self.theme_listbox.bind("<<ListboxSelect>>", self._on_theme_selected)

        button_frame = ttk.Frame(left_frame)
        button_frame.pack(fill="x", pady=(12, 0))
        ttk.Button(button_frame, text="New Theme", command=self._create_new_theme).pack(
            fill="x", pady=(0, 6)
        )
        ttk.Button(button_frame, text="Save Theme", command=self._save_theme).pack(
            fill="x", pady=(0, 6)
        )
        ttk.Button(button_frame, text="Apply Theme", command=self._apply_theme).pack(fill="x")

    def _build_editor_panel(self, parent: ttk.Frame) -> None:
        right_frame = ttk.Frame(parent)
        right_frame.grid(row=0, column=1, sticky="nsew", padx=(18, 0))
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(1, weight=1)

        ttk.Label(right_frame, text="Theme Colors", font=("Segoe UI", 11, "bold")).grid(
            row=0, column=0, sticky="w"
        )

        editor_frame = ttk.Frame(right_frame)
        editor_frame.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        editor_frame.columnconfigure(0, weight=1)
        editor_frame.rowconfigure(1, weight=1)

        self.component_tree = ttk.Treeview(
            editor_frame,
            columns=("component", "color"),
            show="headings",
            height=8,
        )
        self.component_tree.heading("component", text="Component")
        self.component_tree.heading("color", text="Color")
        self.component_tree.column("component", width=260)
        self.component_tree.column("color", width=120, anchor="center")
        self.component_tree.grid(row=0, column=0, sticky="nsew")
        self.component_tree.bind("<<TreeviewSelect>>", self._on_component_selected)

        picker_panel = ttk.Frame(editor_frame, padding=(0, 12, 0, 0))
        picker_panel.grid(row=1, column=0, sticky="nsew")
        picker_panel.columnconfigure(1, weight=1)

        mode_frame = ttk.Labelframe(picker_panel, text="Color Picker Options")
        mode_frame.grid(row=0, column=0, sticky="ew", columnspan=2)

        ttk.Radiobutton(
            mode_frame,
            text="Color Picker",
            variable=self.picker_mode,
            value="screen",
            command=self._show_picker_mode,
        ).grid(row=0, column=0, sticky="w", padx=8, pady=6)
        ttk.Radiobutton(
            mode_frame,
            text="Hex code",
            variable=self.picker_mode,
            value="hex",
            command=self._show_picker_mode,
        ).grid(row=0, column=1, sticky="w", padx=8, pady=6)

        self.mode_container = ttk.Frame(picker_panel)
        self.mode_container.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        self.mode_container.columnconfigure(0, weight=1)

        self.mode_frames: dict[str, ttk.Frame] = {}
        self._build_screen_picker(self.mode_container)
        self._build_hex_picker(self.mode_container)
        self._show_picker_mode()

        preview_frame = ttk.Frame(picker_panel)
        preview_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(12, 0))

        ttk.Label(preview_frame, text="Selected Color:").pack(side="left")
        self.color_swatch = tk.Canvas(preview_frame, width=32, height=18, highlightthickness=1)
        self.color_swatch.pack(side="left", padx=(8, 6))
        self.color_label = ttk.Label(preview_frame, textvariable=self.current_color)
        self.color_label.pack(side="left")

        ttk.Button(
            preview_frame, text="Apply to Component", command=self._apply_color_to_component
        ).pack(side="right")

    def _build_screen_picker(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.columnconfigure(0, weight=1)
        ttk.Label(
            frame,
            text="Use the system color picker to sample a color from anywhere on screen.",
            wraplength=520,
        ).grid(row=0, column=0, sticky="w")
        ttk.Button(frame, text="Pick Color", command=self._pick_screen_color).grid(
            row=1, column=0, sticky="w", pady=(8, 0)
        )
        self.mode_frames["screen"] = frame

    def _build_hex_picker(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.columnconfigure(1, weight=1)
        ttk.Label(frame, text="Enter a hex color code:").grid(row=0, column=0, sticky="w")
        entry = ttk.Entry(frame, textvariable=self.hex_entry, width=14)
        entry.grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Button(frame, text="Apply Hex", command=self._apply_hex_color).grid(
            row=0, column=2, sticky="w", padx=(8, 0)
        )
        self.mode_frames["hex"] = frame

    def _show_picker_mode(self) -> None:
        for frame in self.mode_frames.values():
            frame.grid_forget()
        frame = self.mode_frames.get(self.picker_mode.get())
        if frame:
            frame.grid(row=0, column=0, sticky="ew")

    def _on_theme_selected(self, event: tk.Event) -> None:
        selection = self.theme_listbox.curselection()
        if not selection:
            return
        theme_name = self.theme_listbox.get(selection[0])
        self._load_theme(theme_name)

    def _load_theme(self, theme_name: str) -> None:
        self.selected_theme_name.set(theme_name)
        self.component_tree.delete(*self.component_tree.get_children())
        theme = self.themes.get(theme_name, {})
        for key, label in self.COMPONENTS:
            color = theme.get(key, "#ffffff")
            self.component_tree.insert("", "end", iid=key, values=(label, color))
        self.component_tree.selection_set(self.COMPONENTS[0][0])
        self._set_current_color(theme.get(self.COMPONENTS[0][0], "#ffffff"))

    def _on_component_selected(self, event: tk.Event) -> None:
        selection = self.component_tree.selection()
        if not selection:
            return
        component_id = selection[0]
        self.selected_component = component_id
        color = self.component_tree.set(component_id, "color")
        if color:
            self._set_current_color(color)

    def _set_current_color(self, color: str) -> None:
        self.current_color.set(color)
        self.color_swatch.configure(background=color)
        self.hex_entry.set(color)

    def _pick_screen_color(self) -> None:
        result = colorchooser.askcolor(color=self.current_color.get(), parent=self)
        if result and result[1]:
            self._set_current_color(result[1])

    def _apply_hex_color(self) -> None:
        value = self.hex_entry.get().strip()
        if not value.startswith("#"):
            value = f"#{value}"
        if len(value) != 7 or any(char not in "0123456789abcdefABCDEF" for char in value[1:]):
            messagebox.showerror("Invalid Color", "Enter a valid hex color (e.g. #33AADD).")
            return
        self._set_current_color(value.lower())

    def _apply_color_to_component(self) -> None:
        selection = self.component_tree.selection()
        if not selection:
            messagebox.showinfo("Theme Editor", "Select a component to update.")
            return
        component_id = selection[0]
        theme_name = self.selected_theme_name.get()
        color = self.current_color.get()
        self.component_tree.set(component_id, "color", color)
        self.themes.setdefault(theme_name, {})[component_id] = color

    def _create_new_theme(self) -> None:
        name = simpledialog.askstring("New Theme", "Theme name:", parent=self)
        if not name:
            return
        if name in self.themes:
            messagebox.showerror("New Theme", "That theme name already exists.")
            return
        current_theme = self.themes.get(self.selected_theme_name.get(), {})
        self.themes[name] = dict(current_theme)
        self.theme_names.append(name)
        self.theme_listbox.insert("end", name)
        self.theme_listbox.selection_clear(0, "end")
        self.theme_listbox.selection_set("end")
        self._load_theme(name)

    def _save_theme(self) -> None:
        theme_name = self.selected_theme_name.get()
        if not theme_name:
            return
        messagebox.showinfo("Theme Editor", f"Theme '{theme_name}' saved.")

    def _apply_theme(self) -> None:
        theme_name = self.selected_theme_name.get()
        if not theme_name:
            return
        theme = self.themes.get(theme_name, {})
        if self.apply_theme_callback:
            self.apply_theme_callback(theme)


class WorkflowsDialog(tk.Toplevel):
    def __init__(self, master: tk.Tk) -> None:
        super().__init__(master)
        self.title("Workflows")
        self.geometry("860x520")
        self.resizable(False, False)

        self.workflow_names = self._load_workflows()
        self.description_var = tk.StringVar(value="Select a workflow to see details.")

        container = ttk.Frame(self, padding=12)
        container.grid(sticky="nsew")
        container.columnconfigure(1, weight=1)
        container.rowconfigure(1, weight=1)

        ttk.Label(container, text="Workflows").grid(row=0, column=0, sticky="w")
        ttk.Label(container, text="Workflow description").grid(row=0, column=1, sticky="w")

        list_frame = ttk.Frame(container)
        list_frame.grid(row=1, column=0, rowspan=2, sticky="nsw", padx=(0, 16))
        self.workflow_list = tk.Listbox(list_frame, height=18, width=26, exportselection=False)
        list_scroll = ttk.Scrollbar(list_frame, orient="vertical", command=self.workflow_list.yview)
        self.workflow_list.configure(yscrollcommand=list_scroll.set)
        self.workflow_list.grid(row=0, column=0, sticky="ns")
        list_scroll.grid(row=0, column=1, sticky="ns")
        self.workflow_list.bind("<<ListboxSelect>>", self._on_workflow_selected)

        description_frame = ttk.Frame(container)
        description_frame.grid(row=1, column=1, sticky="ew")
        description_frame.columnconfigure(0, weight=1)
        description_label = ttk.Label(
            description_frame,
            textvariable=self.description_var,
            wraplength=520,
            justify="left",
        )
        description_label.grid(row=0, column=0, sticky="w")

        self.run_button = ttk.Button(description_frame, text="Run Workflow", command=self._run_workflow)
        self.run_button.grid(row=0, column=1, sticky="e", padx=(12, 0))

        preview_frame = ttk.Labelframe(container, text="Workflow preview", padding=12)
        preview_frame.grid(row=2, column=1, sticky="nsew", pady=(12, 0))
        preview_frame.columnconfigure(0, weight=1)
        preview_frame.rowconfigure(0, weight=1)
        ttk.Label(preview_frame, text="Visual diagram of the workflow").grid(
            row=0, column=0, sticky="nsew"
        )

        button_frame = ttk.Frame(container)
        button_frame.grid(row=3, column=1, sticky="e", pady=(12, 0))
        ttk.Button(button_frame, text="Close", command=self.destroy).pack(side="right")

        self._populate_workflows()
        self._update_run_state()
        self.grab_set()
        self.wait_visibility()
        self.focus_set()

    def _load_workflows(self) -> list[str]:
        workflows_dir = Path(__file__).resolve().parent / "workflows"
        if not workflows_dir.is_dir():
            return []
        return sorted(
            path.name for path in workflows_dir.iterdir() if path.is_dir() and not path.name.startswith(".")
        )

    def _populate_workflows(self) -> None:
        self.workflow_list.delete(0, tk.END)
        if not self.workflow_names:
            self.workflow_list.insert(tk.END, "No workflows available yet.")
            self.workflow_list.configure(state="disabled")
            return
        for name in self.workflow_names:
            self.workflow_list.insert(tk.END, name)

    def _on_workflow_selected(self, _event: tk.Event) -> None:
        if not self.workflow_names:
            return
        selection = self.workflow_list.curselection()
        if not selection:
            self.description_var.set("Select a workflow to see details.")
            self._update_run_state()
            return
        selected_name = self.workflow_list.get(selection[0])
        self.description_var.set(f"{selected_name} workflow details will appear here.")
        self._update_run_state()

    def _update_run_state(self) -> None:
        has_selection = bool(self.workflow_list.curselection()) and bool(self.workflow_names)
        self.run_button.configure(state="normal" if has_selection else "disabled")

    def _run_workflow(self) -> None:
        messagebox.showinfo(
            "Workflows",
            "Workflows will be available in a future update.",
            parent=self,
        )


class MediaServerApp:
    def __init__(self, root: tk.Tk, db: LibraryDB) -> None:
        self.root = root
        self.db = db
        self.root.title("Media Server Organizer")
        self.root.geometry("1200x720")
        self.style = ttk.Style(self.root)
        self.themes = self._load_themes()
        self.current_theme = dict(self.themes.get("Default", DEFAULT_THEME))

        self.library_tabs: dict[int, ttk.Frame] = {}
        self.library_views: dict[int, ttk.Treeview] = {}
        self.library_paths: dict[int, str] = {}
        self.current_library: Library | None = None

        self._build_menu()
        self._build_layout()
        self._apply_theme(self.current_theme)
        self._load_libraries()

    def _build_menu(self) -> None:
        menu = tk.Menu(self.root)
        file_menu = tk.Menu(menu, tearoff=0)
        file_menu.add_command(label="New Library...", command=self._open_new_library_dialog)
        file_menu.add_command(label="Open Library Location", command=self._open_current_library_location)
        file_menu.add_command(label="Reload Library View", command=self._refresh_current_library)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menu.add_cascade(label="File", menu=file_menu)

        view_menu = tk.Menu(menu, tearoff=0)
        view_menu.add_command(label="Workflows...", command=self._open_workflows_dialog)
        view_menu.add_separator()
        view_menu.add_command(label="Refresh Library", command=self._refresh_current_library)
        view_menu.add_command(label="Refresh Folder Tree", command=self._refresh_folder_tree)
        view_menu.add_separator()
        view_menu.add_command(label="Expand All Folders", command=lambda: self._set_folder_tree_expanded(True))
        view_menu.add_command(label="Collapse All Folders", command=lambda: self._set_folder_tree_expanded(False))
        menu.add_cascade(label="View", menu=view_menu)

        options_menu = tk.Menu(menu, tearoff=0)
        options_menu.add_command(
            label="Library Management", command=self._open_library_management_dialog
        )
        options_menu.add_command(label="Theme", command=self._open_theme_dialog)
        options_menu.add_command(label="Export", command=lambda: self._show_placeholder("Export"))
        menu.add_cascade(label="Options", menu=options_menu)

        help_menu = tk.Menu(menu, tearoff=0)
        help_menu.add_command(label="About", command=self._show_about)
        menu.add_cascade(label="Help", menu=help_menu)
        self.root.config(menu=menu)

    def _load_themes(self) -> dict[str, dict[str, str]]:
        themes: dict[str, dict[str, str]] = {}
        themes_dir = Path(__file__).resolve().parent / "themes"
        if themes_dir.is_dir():
            for theme_path in sorted(themes_dir.glob("*.json")):
                try:
                    data = json.loads(theme_path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if not isinstance(data, dict):
                    continue
                theme_name = theme_path.stem.replace("_", " ").title()
                themes[theme_name] = {key: str(value) for key, value in data.items()}
        if "Default" not in themes:
            themes["Default"] = dict(DEFAULT_THEME)
        if "Default" in themes:
            themes = {"Default": themes.pop("Default"), **themes}
        return themes

    def _build_layout(self) -> None:
        main_pane = ttk.PanedWindow(self.root, orient="horizontal")
        main_pane.pack(fill="both", expand=True)

        self.left_frame = ttk.Frame(main_pane, width=280, style="Sidebar.TFrame")
        self.left_frame.columnconfigure(0, weight=1)
        self.left_frame.rowconfigure(0, weight=1)
        self.left_frame.rowconfigure(1, weight=0)

        self.folder_tree = ttk.Treeview(self.left_frame, show="tree", style="Sidebar.Treeview")
        folder_scroll = ttk.Scrollbar(
            self.left_frame, orient="vertical", command=self.folder_tree.yview
        )
        self.folder_tree.configure(yscrollcommand=folder_scroll.set)
        self.folder_tree.grid(row=0, column=0, sticky="nsew")
        folder_scroll.grid(row=0, column=1, sticky="ns")
        self.folder_tree.bind("<<TreeviewOpen>>", self._expand_folder_node)
        self.folder_tree.bind("<<TreeviewSelect>>", self._on_folder_tree_selected)
        self.folder_tree.bind("<Double-1>", self._on_folder_tree_double_click)

        self.metadata_frame = ttk.Labelframe(
            self.left_frame, text="File Metadata", padding=8, style="Metadata.TLabelframe"
        )
        self.metadata_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        self.metadata_frame.columnconfigure(0, weight=1)
        self.metadata_frame.rowconfigure(0, weight=1)

        self.metadata_canvas = tk.Canvas(self.metadata_frame, highlightthickness=0)
        self.metadata_scroll = ttk.Scrollbar(
            self.metadata_frame, orient="vertical", command=self.metadata_canvas.yview
        )
        self.metadata_canvas.configure(yscrollcommand=self.metadata_scroll.set)
        self.metadata_canvas.grid(row=0, column=0, sticky="nsew")
        self.metadata_scroll.grid(row=0, column=1, sticky="ns")

        self.metadata_content = ttk.Frame(self.metadata_canvas, style="Metadata.TFrame")
        self.metadata_content.columnconfigure(1, weight=1)
        self.metadata_window = self.metadata_canvas.create_window(
            (0, 0), window=self.metadata_content, anchor="nw"
        )
        self.metadata_content.bind(
            "<Configure>", lambda event: self.metadata_canvas.configure(scrollregion=self.metadata_canvas.bbox("all"))
        )
        self.metadata_canvas.bind(
            "<Configure>", lambda event: self.metadata_canvas.itemconfig(self.metadata_window, width=event.width)
        )

        self.right_frame = ttk.Frame(main_pane)
        self.right_frame.columnconfigure(0, weight=1)
        self.right_frame.rowconfigure(0, weight=1)

        self.notebook = ttk.Notebook(self.right_frame)
        self.notebook.grid(row=0, column=0, sticky="nsew")
        self.notebook.bind("<<NotebookTabChanged>>", self._handle_tab_changed)

        self.new_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.new_tab, text="+")

        main_pane.add(self.left_frame, weight=1)
        main_pane.add(self.right_frame, weight=4)

    def _load_libraries(self) -> None:
        for library in self.db.fetch_libraries():
            self._create_library_tab(library)
        if self.library_tabs:
            first_library = next(iter(self.library_tabs))
            self.notebook.select(self.library_tabs[first_library])
            self._set_current_library(self.db.fetch_libraries()[0])

    def _open_new_library_dialog(self) -> None:
        dialog = NewLibraryDialog(self.root)
        self.root.wait_window(dialog)
        if dialog.result:
            library = self.db.add_library(
                name=dialog.result["name"],
                library_type=dialog.result["library_type"],
                path=dialog.result["path"],
                host=dialog.result["host"],
                username=dialog.result["username"],
            )
            self._create_library_tab(library)
            self.notebook.select(self.library_tabs[library.library_id])
            self._set_current_library(library)

    def _open_library_management_dialog(self) -> None:
        dialog = LibraryManagementDialog(self.root, self.db)
        self.root.wait_window(dialog)
        self._sync_libraries()

    def _open_workflows_dialog(self) -> None:
        dialog = WorkflowsDialog(self.root)
        self.root.wait_window(dialog)

    def _create_library_tab(self, library: Library) -> None:
        frame = ttk.Frame(self.notebook)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        tree = ttk.Treeview(frame, columns=("type", "location"), show="headings", style="Library.Treeview")
        tree.heading("type", text="Type")
        tree.heading("location", text="Location")
        tree.column("type", width=120, anchor="w")
        tree.column("location", width=400, anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")

        scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

        tree.bind("<<TreeviewSelect>>", self._on_library_item_selected)
        tree.bind("<Double-1>", self._on_library_item_double_click)

        self.library_tabs[library.library_id] = frame
        self.library_views[library.library_id] = tree
        self.notebook.insert(self.new_tab, frame, text=library.name)
        self.library_paths.setdefault(library.library_id, library.path)
        self._populate_library_view(library, self.library_paths[library.library_id])

    def _sync_libraries(self) -> None:
        libraries = self.db.fetch_libraries()
        library_map = {library.library_id: library for library in libraries}

        for library_id in list(self.library_tabs.keys()):
            if library_id not in library_map:
                self.notebook.forget(self.library_tabs[library_id])
                self.library_tabs.pop(library_id, None)
                self.library_views.pop(library_id, None)
                self.library_paths.pop(library_id, None)

        for library in libraries:
            if library.library_id not in self.library_tabs:
                self._create_library_tab(library)
            else:
                self.notebook.tab(self.library_tabs[library.library_id], text=library.name)
                self.library_paths[library.library_id] = library.path

        if self.current_library and self.current_library.library_id in library_map:
            self._set_current_library(library_map[self.current_library.library_id])
        elif libraries:
            self._set_current_library(libraries[0])
            self.notebook.select(self.library_tabs[libraries[0].library_id])
        else:
            self.current_library = None
            self._clear_metadata()
            self._refresh_folder_tree()

    def _populate_library_view(self, library: Library, path: str) -> None:
        tree = self.library_views[library.library_id]
        for item in tree.get_children():
            tree.delete(item)

        if library.library_type == "remote":
            tree.insert("", "end", values=("Remote Library", f"{library.username or 'user'}@{library.host}"))
            tree.insert("", "end", values=("Path", library.path))
            return

        if not os.path.isdir(path):
            tree.insert("", "end", values=("Missing", path))
            return

        try:
            entries = sorted(os.listdir(path))
        except OSError as exc:
            tree.insert("", "end", values=("Error", str(exc)))
            return

        for entry in entries:
            full_path = os.path.join(path, entry)
            entry_type = "Folder" if os.path.isdir(full_path) else "File"
            tree.insert("", "end", values=(entry_type, full_path))

    def _handle_tab_changed(self, _event: tk.Event) -> None:
        selected = self.notebook.select()
        if selected == str(self.new_tab):
            self._open_new_library_dialog()
            if self.current_library:
                self.notebook.select(self.library_tabs[self.current_library.library_id])
            return

        for library_id, frame in self.library_tabs.items():
            if str(frame) == selected:
                library = next(
                    (item for item in self.db.fetch_libraries() if item.library_id == library_id),
                    None,
                )
                if library:
                    self._set_current_library(library)
                break

    def _set_current_library(self, library: Library) -> None:
        self.current_library = library
        self.library_paths.setdefault(library.library_id, library.path)
        self._populate_library_view(library, self.library_paths[library.library_id])
        self._refresh_folder_tree()
        self._clear_metadata()

    def _refresh_current_library(self) -> None:
        if not self.current_library:
            return
        current_path = self.library_paths.get(
            self.current_library.library_id, self.current_library.path
        )
        self._populate_library_view(self.current_library, current_path)
        self._refresh_folder_tree()

    def _refresh_folder_tree(self) -> None:
        for item in self.folder_tree.get_children():
            self.folder_tree.delete(item)

        if not self.current_library:
            return
        if self.current_library.library_type != "local":
            self.folder_tree.insert("", "end", text="Remote libraries unavailable for browsing")
            return

        root_path = self.current_library.path
        root_node = self.folder_tree.insert("", "end", text=root_path, open=True, values=(root_path,))
        self._populate_folder_children(root_node, root_path)

    def _populate_folder_children(self, parent_id: str, path: str) -> None:
        try:
            entries = sorted(os.listdir(path))
        except OSError:
            return
        for entry in entries:
            full_path = os.path.join(path, entry)
            if os.path.isdir(full_path):
                node = self.folder_tree.insert(parent_id, "end", text=entry, values=(full_path,))
                self.folder_tree.insert(node, "end", text="loading...")

    def _expand_folder_node(self, event: tk.Event) -> None:
        node_id = self.folder_tree.focus()
        children = self.folder_tree.get_children(node_id)
        if len(children) == 1 and self.folder_tree.item(children[0], "text") == "loading...":
            self.folder_tree.delete(children[0])
            path = self.folder_tree.item(node_id, "values")[0]
            self._populate_folder_children(node_id, path)

    def _on_folder_tree_selected(self, _event: tk.Event) -> None:
        selection = self.folder_tree.selection()
        if not selection:
            return
        node_id = selection[0]
        values = self.folder_tree.item(node_id, "values")
        if not values:
            return
        path = values[0]
        if os.path.isdir(path):
            entry_type = "Folder"
        elif os.path.isfile(path):
            entry_type = "File"
        else:
            entry_type = "Unknown"
        self._set_metadata_rows(self._gather_metadata(path, entry_type))

    def _on_folder_tree_double_click(self, _event: tk.Event) -> None:
        selection = self.folder_tree.selection()
        if not selection:
            return
        node_id = selection[0]
        values = self.folder_tree.item(node_id, "values")
        if not values:
            return
        path = values[0]
        if os.path.isdir(path):
            self._navigate_to_path(path)

    def _on_library_item_selected(self, _event: tk.Event) -> None:
        if not self.current_library:
            return
        tree = self.library_views[self.current_library.library_id]
        selection = tree.selection()
        if not selection:
            return
        item = tree.item(selection[0])
        entry_type, location = item["values"]
        self._set_metadata_rows(self._gather_metadata(location, entry_type))

    def _on_library_item_double_click(self, _event: tk.Event) -> None:
        if not self.current_library or self.current_library.library_type != "local":
            return
        tree = self.library_views[self.current_library.library_id]
        selection = tree.selection()
        if not selection:
            return
        item = tree.item(selection[0])
        entry_type, location = item["values"]
        if entry_type == "Folder" and os.path.isdir(location):
            self._navigate_to_path(location)

    def _navigate_to_path(self, path: str) -> None:
        if not self.current_library or self.current_library.library_type != "local":
            return
        if not os.path.isdir(path):
            return
        self.library_paths[self.current_library.library_id] = path
        self._populate_library_view(self.current_library, path)
        self._clear_metadata()

    def _set_folder_tree_expanded(self, expanded: bool) -> None:
        def recurse(node: str) -> None:
            self.folder_tree.item(node, open=expanded)
            for child in self.folder_tree.get_children(node):
                recurse(child)

        for root in self.folder_tree.get_children(""):
            recurse(root)

    def _open_current_library_location(self) -> None:
        if not self.current_library or self.current_library.library_type != "local":
            messagebox.showinfo(
                "Open Library Location", "Select a local library to open its folder."
            )
            return
        path = self.library_paths.get(self.current_library.library_id, self.current_library.path)
        if not os.path.isdir(path):
            messagebox.showerror("Open Library Location", "Library path is unavailable.")
            return
        if sys.platform.startswith("win"):
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.run(["open", path], check=False)
        else:
            subprocess.run(["xdg-open", path], check=False)

    def _open_theme_dialog(self) -> None:
        ThemeEditorDialog(self.root, self.themes, self._apply_theme)

    def _apply_theme(self, theme: dict[str, str]) -> None:
        self.current_theme = dict(theme)
        window_bg = theme.get("window_background", self.root.cget("bg"))
        sidebar_bg = theme.get("sidebar_background", window_bg)
        toolbar_bg = theme.get("toolbar_background", window_bg)
        tree_bg = theme.get("treeview_background", "white")
        metadata_bg = theme.get("metadata_background", window_bg)
        accent = theme.get("accent_color", "#3b74ff")
        text_color = theme.get("text_color", "black")

        self.root.configure(background=window_bg)
        self.style.configure("TFrame", background=window_bg)
        self.style.configure("Sidebar.TFrame", background=sidebar_bg)
        self.style.configure("Metadata.TFrame", background=metadata_bg)
        self.style.configure("TLabel", background=window_bg, foreground=text_color)
        self.style.configure("Sidebar.TLabel", background=sidebar_bg, foreground=text_color)
        self.style.configure("Metadata.TLabel", background=metadata_bg, foreground=text_color)
        self.style.configure("TLabelframe", background=window_bg, foreground=text_color)
        self.style.configure(
            "TLabelframe.Label", background=window_bg, foreground=text_color
        )
        self.style.configure(
            "Metadata.TLabelframe", background=metadata_bg, foreground=text_color
        )
        self.style.configure(
            "Metadata.TLabelframe.Label", background=metadata_bg, foreground=text_color
        )
        self.style.configure(
            "Treeview",
            background=tree_bg,
            fieldbackground=tree_bg,
            foreground=text_color,
        )
        self.style.configure(
            "Treeview.Heading",
            background=toolbar_bg,
            foreground=text_color,
        )
        self.style.configure(
            "Sidebar.Treeview",
            background=tree_bg,
            fieldbackground=tree_bg,
            foreground=text_color,
        )
        self.style.configure(
            "Library.Treeview",
            background=tree_bg,
            fieldbackground=tree_bg,
            foreground=text_color,
        )
        self.style.configure(
            "Accent.TButton",
            background=accent,
            foreground=text_color,
        )

        if hasattr(self, "metadata_canvas"):
            self.metadata_canvas.configure(background=metadata_bg)

    def _show_placeholder(self, title: str) -> None:
        messagebox.showinfo(title, f"{title} options will be available in a future update.")

    def _show_about(self) -> None:
        messagebox.showinfo(
            "About Media Server Organizer",
            "Media Server Organizer\nManage and explore your media libraries.",
        )

    def _set_metadata_rows(self, rows: list[tuple[str, str]]) -> None:
        for child in self.metadata_content.winfo_children():
            child.destroy()
        if not rows:
            rows = [("Metadata", "—")]
        for index, (label, value) in enumerate(rows):
            ttk.Label(self.metadata_content, text=f"{label}:", style="Metadata.TLabel").grid(
                row=index, column=0, sticky="nw", padx=(0, 8), pady=2
            )
            value_label = ttk.Label(
                self.metadata_content,
                text=value or "—",
                wraplength=240,
                justify="left",
                style="Metadata.TLabel",
            )
            value_label.grid(row=index, column=1, sticky="w", pady=2)

    def _clear_metadata(self) -> None:
        self._set_metadata_rows([("Metadata", "Select a file or folder to view details.")])

    def _gather_metadata(self, path: str, entry_type: str) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = [
            ("Title", os.path.basename(path) or path),
            ("Path", path),
            ("Type", entry_type),
        ]

        if not os.path.exists(path):
            rows.append(("Status", "Unavailable"))
            return rows

        if os.path.isdir(path):
            try:
                entries = os.listdir(path)
            except OSError as exc:
                rows.append(("Error", str(exc)))
            else:
                rows.append(("Item Count", str(len(entries))))
            return rows

        try:
            stats = os.stat(path)
        except OSError as exc:
            rows.append(("Error", str(exc)))
            return rows

        rows.extend(
            [
                ("Size", f"{self._format_size(stats.st_size)} ({stats.st_size:,} bytes)"),
                ("Modified", self._format_timestamp(stats.st_mtime)),
                ("Created", self._format_timestamp(stats.st_ctime)),
            ]
        )

        extension = os.path.splitext(path)[1].lower()
        if extension:
            rows.append(("Extension", extension))

        if self._is_media_file(extension):
            rows.extend(self._probe_media(path))

        return rows

    @staticmethod
    def _format_timestamp(timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        units = ["B", "KB", "MB", "GB", "TB"]
        size = float(size_bytes)
        for unit in units:
            if size < 1024 or unit == units[-1]:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"

    @staticmethod
    def _is_media_file(extension: str) -> bool:
        audio_exts = {".mp3", ".flac", ".aac", ".m4a", ".wav", ".ogg", ".opus"}
        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".webm", ".m4v"}
        return extension in audio_exts or extension in video_exts

    def _probe_media(self, path: str) -> list[tuple[str, str]]:
        if not shutil.which("ffprobe"):
            return [("Media Info", "ffprobe not available on this system.")]

        try:
            result = subprocess.run(
                [
                    "ffprobe",
                    "-v",
                    "error",
                    "-print_format",
                    "json",
                    "-show_format",
                    "-show_streams",
                    path,
                ],
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except (subprocess.SubprocessError, OSError) as exc:
            return [("Media Info", f"Unable to read media metadata: {exc}")]

        if not result.stdout:
            return [("Media Info", "Metadata failed to parse.")]

        try:
            data = json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError):
            return [("Media Info", "Metadata failed to parse.")]

        rows: list[tuple[str, str]] = []
        format_info = data.get("format", {})
        if format_info:
            if duration := format_info.get("duration"):
                rows.append(("Duration", self._format_duration(duration)))
            if bitrate := format_info.get("bit_rate"):
                rows.append(("Overall Bitrate", f"{self._format_bitrate(bitrate)}"))
            if container := format_info.get("format_name"):
                rows.append(("Container", container))

        streams = data.get("streams", [])
        video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
        audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

        if video_stream:
            rows.extend(self._video_stream_rows(video_stream))
        if audio_stream:
            rows.extend(self._audio_stream_rows(audio_stream))

        tags = {}
        tags.update(format_info.get("tags", {}) if isinstance(format_info.get("tags"), dict) else {})
        if audio_stream and isinstance(audio_stream.get("tags"), dict):
            tags.update(audio_stream["tags"])
        if video_stream and isinstance(video_stream.get("tags"), dict):
            tags.update(video_stream["tags"])

        for key, value in self._format_media_tags(tags).items():
            rows.append((key, value))

        return rows

    @staticmethod
    def _format_duration(duration: str) -> str:
        try:
            total_seconds = float(duration)
        except (TypeError, ValueError):
            return duration
        minutes, seconds = divmod(int(total_seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours:d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:d}:{seconds:02d}"

    @staticmethod
    def _format_bitrate(bit_rate: str) -> str:
        try:
            return f"{int(bit_rate) / 1000:.0f} kbps"
        except (TypeError, ValueError):
            return bit_rate

    @staticmethod
    def _video_stream_rows(stream: dict[str, object]) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = [("Media Type", "Video")]
        if codec := stream.get("codec_name"):
            rows.append(("Video Codec", str(codec)))
        width = stream.get("width")
        height = stream.get("height")
        if width and height:
            rows.append(("Resolution", f"{width}x{height}"))
        fps = MediaServerApp._parse_frame_rate(stream.get("avg_frame_rate"))
        if not fps:
            fps = MediaServerApp._parse_frame_rate(stream.get("r_frame_rate"))
        if fps:
            rows.append(("FPS", fps))
        if bit_rate := stream.get("bit_rate"):
            rows.append(("Video Bitrate", MediaServerApp._format_bitrate(str(bit_rate))))
        return rows

    @staticmethod
    def _audio_stream_rows(stream: dict[str, object]) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = [("Media Type", "Audio")]
        if codec := stream.get("codec_name"):
            rows.append(("Audio Codec", str(codec)))
        if bit_rate := stream.get("bit_rate"):
            rows.append(("Audio Bitrate", MediaServerApp._format_bitrate(str(bit_rate))))
        if sample_rate := stream.get("sample_rate"):
            rows.append(("Sample Rate", f"{int(sample_rate):,} Hz"))
        if channels := stream.get("channels"):
            rows.append(("Channels", str(channels)))
        return rows

    @staticmethod
    def _parse_frame_rate(rate: object) -> str | None:
        if not rate or not isinstance(rate, str) or rate == "0/0":
            return None
        try:
            value = float(Fraction(rate))
        except (ValueError, ZeroDivisionError):
            return None
        return f"{value:.2f}"

    @staticmethod
    def _format_media_tags(tags: dict[str, str]) -> dict[str, str]:
        preferred = {
            "artist": "Artist",
            "album": "Album",
            "title": "Track Title",
            "genre": "Genre",
            "track": "Track",
            "date": "Year",
        }
        formatted: dict[str, str] = {}
        seen = set()
        for key, label in preferred.items():
            value = tags.get(key) or tags.get(key.upper())
            if value:
                formatted[label] = str(value)
                seen.add(key)
                seen.add(key.upper())
        for key, value in sorted(tags.items()):
            if key in seen:
                continue
            formatted[f"Tag ({key})"] = str(value)
        return formatted

    def _edit_metadata_value(self, event: tk.Event) -> None:
        label_widget = event.widget
        field_name = next(
            (key for key, label in self.metadata_labels.items() if label == label_widget), None
        )
        if not field_name:
            return
        current_value = label_widget.cget("text")
        new_value = simpledialog.askstring(
            "Edit Metadata", f"Update {field_name}:", initialvalue=current_value, parent=self.root
        )
        if new_value is not None:
            label_widget.config(text=new_value)


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DB_DEFAULT_PATH, help="SQLite database location.")
    parser.add_argument("--nogui", action="store_true", help="Run in CLI-only mode.")
    args = parser.parse_args()

    if args.nogui:
        print("GUI disabled. Provide --db to change database location.")
        return

    root = tk.Tk()
    db = LibraryDB(args.db)
    try:
        MediaServerApp(root, db)
        root.mainloop()
    finally:
        db.close()


if __name__ == "__main__":
    run()
