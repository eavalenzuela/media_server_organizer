import argparse
import os
import sqlite3
import sys
import tkinter as tk
from dataclasses import dataclass
from tkinter import filedialog, messagebox, simpledialog, ttk


DB_DEFAULT_PATH = os.path.join(os.path.expanduser("~"), ".media_server_organizer.db")


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


class MediaServerApp:
    def __init__(self, root: tk.Tk, db: LibraryDB) -> None:
        self.root = root
        self.db = db
        self.root.title("Media Server Organizer")
        self.root.geometry("1200x720")

        self.library_tabs: dict[int, ttk.Frame] = {}
        self.library_views: dict[int, ttk.Treeview] = {}
        self.current_library: Library | None = None

        self._build_menu()
        self._build_layout()
        self._load_libraries()

    def _build_menu(self) -> None:
        menu = tk.Menu(self.root)
        file_menu = tk.Menu(menu, tearoff=0)
        file_menu.add_command(label="New Library...", command=self._open_new_library_dialog)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menu.add_cascade(label="File", menu=file_menu)

        view_menu = tk.Menu(menu, tearoff=0)
        view_menu.add_command(label="Refresh Library", command=self._refresh_current_library)
        menu.add_cascade(label="View", menu=view_menu)
        self.root.config(menu=menu)

    def _build_layout(self) -> None:
        main_pane = ttk.PanedWindow(self.root, orient="horizontal")
        main_pane.pack(fill="both", expand=True)

        left_frame = ttk.Frame(main_pane, width=280)
        left_frame.columnconfigure(0, weight=1)
        left_frame.rowconfigure(0, weight=1)
        left_frame.rowconfigure(1, weight=0)

        self.folder_tree = ttk.Treeview(left_frame, show="tree")
        folder_scroll = ttk.Scrollbar(left_frame, orient="vertical", command=self.folder_tree.yview)
        self.folder_tree.configure(yscrollcommand=folder_scroll.set)
        self.folder_tree.grid(row=0, column=0, sticky="nsew")
        folder_scroll.grid(row=0, column=1, sticky="ns")
        self.folder_tree.bind("<<TreeviewOpen>>", self._expand_folder_node)
        self.folder_tree.bind("<<TreeviewSelect>>", self._on_folder_tree_selected)

        self.metadata_frame = ttk.Labelframe(left_frame, text="File Metadata", padding=8)
        self.metadata_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 0))

        self.metadata_labels: dict[str, ttk.Label] = {}
        for index, label in enumerate(["Title", "Path", "Type", "Notes"]):
            ttk.Label(self.metadata_frame, text=f"{label}:").grid(
                row=index, column=0, sticky="w", padx=(0, 6), pady=2
            )
            value_label = ttk.Label(self.metadata_frame, text="—", width=26)
            value_label.grid(row=index, column=1, sticky="w", pady=2)
            value_label.bind("<Double-Button-1>", self._edit_metadata_value)
            self.metadata_labels[label] = value_label

        right_frame = ttk.Frame(main_pane)
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(0, weight=1)

        self.notebook = ttk.Notebook(right_frame)
        self.notebook.grid(row=0, column=0, sticky="nsew")
        self.notebook.bind("<<NotebookTabChanged>>", self._handle_tab_changed)

        self.new_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.new_tab, text="+")

        main_pane.add(left_frame, weight=1)
        main_pane.add(right_frame, weight=4)

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

    def _create_library_tab(self, library: Library) -> None:
        frame = ttk.Frame(self.notebook)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        tree = ttk.Treeview(frame, columns=("type", "location"), show="headings")
        tree.heading("type", text="Type")
        tree.heading("location", text="Location")
        tree.column("type", width=120, anchor="w")
        tree.column("location", width=400, anchor="w")
        tree.grid(row=0, column=0, sticky="nsew")

        scroll = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        scroll.grid(row=0, column=1, sticky="ns")

        tree.bind("<<TreeviewSelect>>", self._on_library_item_selected)

        self.library_tabs[library.library_id] = frame
        self.library_views[library.library_id] = tree
        self.notebook.insert(self.new_tab, frame, text=library.name)
        self._populate_library_view(library)

    def _populate_library_view(self, library: Library) -> None:
        tree = self.library_views[library.library_id]
        for item in tree.get_children():
            tree.delete(item)

        if library.library_type == "remote":
            tree.insert("", "end", values=("Remote Library", f"{library.username or 'user'}@{library.host}"))
            tree.insert("", "end", values=("Path", library.path))
            return

        if not os.path.isdir(library.path):
            tree.insert("", "end", values=("Missing", library.path))
            return

        try:
            entries = sorted(os.listdir(library.path))
        except OSError as exc:
            tree.insert("", "end", values=("Error", str(exc)))
            return

        for entry in entries:
            full_path = os.path.join(library.path, entry)
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
        self._refresh_folder_tree()
        self._clear_metadata()

    def _refresh_current_library(self) -> None:
        if not self.current_library:
            return
        self._populate_library_view(self.current_library)
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
        self._update_metadata(
            {
                "Title": os.path.basename(path) or path,
                "Path": path,
                "Type": entry_type,
                "Notes": "Double-click to edit.",
            }
        )

    def _on_library_item_selected(self, _event: tk.Event) -> None:
        if not self.current_library:
            return
        tree = self.library_views[self.current_library.library_id]
        selection = tree.selection()
        if not selection:
            return
        item = tree.item(selection[0])
        entry_type, location = item["values"]
        self._update_metadata(
            {
                "Title": os.path.basename(location) or location,
                "Path": location,
                "Type": entry_type,
                "Notes": "Double-click to edit.",
            }
        )

    def _update_metadata(self, data: dict[str, str]) -> None:
        for key, label in self.metadata_labels.items():
            label.config(text=data.get(key, "—"))

    def _clear_metadata(self) -> None:
        self._update_metadata({})

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
