from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import os
from typing import List, Dict, Any, Tuple

from flask import Flask, render_template, request, redirect, url_for, jsonify, flash
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func

# Optional: pandas for CSV/Excel import
try:
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover
    pd = None  # guarded at runtime


# -----------------------------------------------------------------------------
# App factory & configuration
# -----------------------------------------------------------------------------
def create_app() -> Flask:
    app = Flask(__name__)

    # SQLite by default; change DATABASE_URL env var if needed
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", "sqlite:///expenses.db")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-change-me")
    app.config["MAX_CONTENT_LENGTH"] = 12 * 1024 * 1024  # 12 MB uploads

    db.init_app(app)

    # Ensure tables exist
    with app.app_context():
        db.create_all()

    register_routes(app)
    register_cli(app)
    return app


db = SQLAlchemy()


# -----------------------------------------------------------------------------
# Database models
# -----------------------------------------------------------------------------
class Category(db.Model):
    __tablename__ = "categories"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), nullable=False, unique=True, index=True)

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "name": self.name}

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Category {self.id} {self.name}>"


class Transaction(db.Model):
    __tablename__ = "transactions"

    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.Date, nullable=False, index=True)
    category = db.Column(db.String(64), nullable=False, index=True)
    name = db.Column(db.String(255), nullable=False)
    amount_kd = db.Column(db.Numeric(10, 3), nullable=False)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "date": self.date.isoformat(),
            "category": self.category,
            "name": self.name,
            "amount_kd": float(self.amount_kd),
        }

    def __repr__(self) -> str:  # pragma: no cover
        return f"<Txn {self.id} {self.date} {self.category} {self.amount_kd} KD>"


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
ALLOWED_EXTS = {".csv", ".xlsx", ".xls"}


def _ext(filename: str) -> str:
    return os.path.splitext(filename or "")[1].lower()


def _norm(s: str) -> str:
    """Normalize header names (case/space/underscore-insensitive)."""
    return " ".join((s or "").strip().lower().replace("_", " ").split())


def _parse_date(s: str | None) -> date:
    if not s:
        return date.today()
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def _parse_amount(s: str | None) -> Decimal:
    return Decimal((s or "").replace(",", "").strip() or "0")


# --- shared mapping for uploads ---
REQUIRED_NAMES: Dict[str, List[str]] = {
    "date": ["date"],
    "category": ["category"],
    "description": ["transaction description", "description", "name"],
    "amount": ["amount (kwd)", "amount", "amount kd", "amount_kd"],
}


def _read_tabular_file_to_df(file) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Read CSV/Excel into a DataFrame and return the detected column mapping."""
    if pd is None:
        raise RuntimeError("File import requires pandas. Install with: pip install pandas openpyxl")
    ext = _ext(file.filename)
    if ext not in ALLOWED_EXTS:
        raise RuntimeError("Unsupported file type. Please upload .csv, .xlsx, or .xls.")

    # Read
    if ext == ".csv":
        df = pd.read_csv(file)
    else:
        file.stream.seek(0)
        df = pd.read_excel(file)  # requires openpyxl

    # Build column map (incoming -> canonical)
    colmap: Dict[str, str] = {}
    for col in df.columns:
        n = _norm(str(col))
        for key, alts in REQUIRED_NAMES.items():
            if n in alts and key not in colmap:
                colmap[key] = col

    missing = [k for k in REQUIRED_NAMES if k not in colmap]
    if missing:
        want = "Date, Category, Transaction Description, Amount (KWD)"
        raise RuntimeError(
            f"Missing required column(s): {', '.join(missing)}. Your header row should include: {want}."
        )

    # Normalize columns to: date, category, name, amount_kd
    df = df.rename(
        columns={
            colmap["date"]: "date",
            colmap["category"]: "category",
            colmap["description"]: "name",
            colmap["amount"]: "amount_kd",
        }
    )
    return df, {
        "date": colmap["date"],
        "category": colmap["category"],
        "name": colmap["description"],
        "amount_kd": colmap["amount"],
    }


def _df_to_preview_rows(df: "pd.DataFrame") -> Tuple[List[Dict[str, Any]], int]:
    """Coerce types; return list of rows ready for client preview (no DB writes)."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    before = len(df)
    df = df.dropna(subset=["date", "category", "name", "amount_kd"])  # type: ignore[arg-type]
    skipped = before - len(df)

    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        try:
            d: date = r["date"]  # type: ignore[assignment]
            cat = str(r["category"]).strip()
            nm = str(r["name"]).strip()
            amt = _parse_amount(str(r["amount_kd"]))
            rows.append(
                {
                    "date": d.isoformat(),
                    "category": cat,
                    "name": nm,
                    "amount_kd": f"{amt:.3f}",
                }
            )
        except Exception:
            skipped += 1
    return rows, skipped


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
def register_routes(app: Flask) -> None:
    @app.route("/")
    def dashboard():
        # expects templates/dashboard.html
        return render_template("dashboard.html")

    @app.route("/transactions", methods=["GET", "POST"])
    def transactions():
        # Add a transaction (simple form)
        if request.method == "POST":
            try:
                txn_date = _parse_date(request.form.get("date"))
                category = (request.form.get("category") or "").strip()
                name = (request.form.get("name") or "").strip()
                amount = _parse_amount(request.form.get("amount_kd"))
                if not category or not name:
                    raise ValueError("Please provide category and description.")
            except ValueError as ve:
                flash(str(ve), "danger")
            except Exception as e:  # noqa: BLE001
                flash(f"Invalid input: {e}", "danger")
            else:
                try:
                    db.session.add(Transaction(date=txn_date, category=category, name=name, amount_kd=amount))
                    db.session.commit()
                    flash("Transaction added!", "success")
                    return redirect(url_for("transactions"))
                except Exception as e:  # noqa: BLE001
                    db.session.rollback()
                    flash(f"Unexpected error: {e}", "danger")

        # Page data (latest 100)
        items: List[Transaction] = (
            Transaction.query.order_by(Transaction.date.desc(), Transaction.id.desc()).limit(100).all()
        )
        categories: List[Category] = Category.query.order_by(Category.name.asc()).all()
        return render_template("transactions.html", items=items, categories=categories)

    # -------- Delete a transaction --------
    @app.route("/transactions/<int:txn_id>/delete", methods=["POST"])
    def delete_transaction(txn_id: int):
        txn = Transaction.query.get_or_404(txn_id)
        db.session.delete(txn)
        db.session.commit()
        flash("Transaction deleted.", "success")
        return redirect(url_for("transactions"))

    # ----------------- Upload (legacy, immediate import) -----------------
    @app.route("/transactions/upload", methods=["POST"])
    def upload_transactions():
        file = request.files.get("file")
        if not file or not file.filename:
            flash("Please choose a CSV or Excel file.", "danger")
            return redirect(url_for("transactions"))

        try:
            df, _ = _read_tabular_file_to_df(file)
            rows, skipped = _df_to_preview_rows(df)
        except Exception as e:  # noqa: BLE001
            flash(str(e), "danger")
            return redirect(url_for("transactions"))

        # Immediate commit (legacy behavior)
        imported = 0
        new_cats = set()
        try:
            txns: List[Transaction] = []
            for r in rows:
                txns.append(
                    Transaction(
                        date=_parse_date(r["date"]),
                        category=r["category"],
                        name=r["name"],
                        amount_kd=_parse_amount(r["amount_kd"]),
                    )
                )
                new_cats.add(r["category"])

            if txns:
                db.session.bulk_save_objects(txns)
            if new_cats:
                existing = {c.name.lower() for c in Category.query.all()}
                for c in new_cats:
                    if c.lower() not in existing:
                        db.session.add(Category(name=c))
            db.session.commit()
            imported = len(txns)
            flash(f"Imported {imported} transaction(s). Skipped {skipped}.", "success")
        except Exception as e:  # noqa: BLE001
            db.session.rollback()
            flash(f"Import failed: {e}", "danger")

        return redirect(url_for("transactions"))

    # ----------------- New: Preview upload (no DB write) -----------------
    @app.route("/transactions/upload-preview", methods=["POST"])
    def upload_preview():
        file = request.files.get("file")
        if not file or not file.filename:
            return jsonify({"ok": False, "error": "Please choose a CSV or Excel file."}), 400
        try:
            df, original_cols = _read_tabular_file_to_df(file)
            rows, skipped = _df_to_preview_rows(df)
            # Limit very large previews to protect browser; still tell full counts
            preview_cap = 2000
            preview_rows = rows[:preview_cap]
            capped = len(rows) > preview_cap
            return jsonify(
                {
                    "ok": True,
                    "count": len(rows),
                    "skipped": skipped,
                    "capped": capped,
                    "preview_rows": preview_rows,
                    "original_columns": original_cols,  # what we matched
                    "schema": ["date", "category", "name", "amount_kd"],
                    "note": "Edit rows client-side, then POST to /transactions/import-commit.",
                }
            )
        except Exception as e:  # noqa: BLE001
            return jsonify({"ok": False, "error": str(e)}), 400

    # ----------------- New: Commit edited preview -----------------
    @app.route("/transactions/import-commit", methods=["POST"])
    def import_commit():
        payload = request.get_json(silent=True) or {}
        rows = payload.get("rows") or []
        if not isinstance(rows, list) or not rows:
            return jsonify({"ok": False, "error": "No rows provided."}), 400

        txns: List[Transaction] = []
        new_cats = set()
        imported = 0
        skipped = 0

        for r in rows:
            try:
                d = _parse_date(r.get("date"))
                cat = (r.get("category") or "").strip()
                nm = (r.get("name") or "").strip()
                amt = _parse_amount(r.get("amount_kd"))
                if not cat or not nm:
                    raise ValueError("Category and description are required.")
                txns.append(Transaction(date=d, category=cat, name=nm, amount_kd=amt))
                new_cats.add(cat)
                imported += 1
            except Exception:
                skipped += 1

        try:
            if txns:
                db.session.bulk_save_objects(txns)
            if new_cats:
                existing = {c.name.lower() for c in Category.query.all()}
                for c in new_cats:
                    if c.lower() not in existing:
                        db.session.add(Category(name=c))
            db.session.commit()
            return jsonify({"ok": True, "imported": imported, "skipped": skipped})
        except Exception as e:  # noqa: BLE001
            db.session.rollback()
            return jsonify({"ok": False, "error": f"Commit failed: {e}"}), 500

    # ----------------- Categories API + delete -----------------
    @app.route("/api/categories", methods=["GET", "POST"])
    def api_categories():
        if request.method == "GET":
            cats = Category.query.order_by(Category.name.asc()).all()
            return jsonify([c.to_dict() for c in cats])

        payload = request.get_json(silent=True) or {}
        name = (payload.get("name") or request.form.get("name") or "").strip()
        if not name:
            return jsonify({"error": "Name is required."}), 400

        existing = Category.query.filter(func.lower(Category.name) == name.lower()).first()
        if existing:
            return jsonify(existing.to_dict()), 200

        cat = Category(name=name)
        db.session.add(cat)
        db.session.commit()
        return jsonify(cat.to_dict()), 201

    @app.route("/categories/<int:cat_id>/delete", methods=["POST"])
    def delete_category(cat_id: int):
        cat = Category.query.get_or_404(cat_id)
        db.session.delete(cat)
        db.session.commit()
        flash("Category deleted.", "success")
        return redirect(url_for("transactions"))

    # ----------------- Charts / API -----------------
    @app.route("/api/spend-by-category")
    def api_spend_by_category():
        rows = (
            db.session.query(Transaction.category, func.sum(Transaction.amount_kd))
            .group_by(Transaction.category)
            .order_by(func.sum(Transaction.amount_kd).desc())
            .all()
        )
        return jsonify({cat: float(total) for cat, total in rows})

    @app.route("/api/spend-by-month")
    def api_spend_by_month():
        ym = func.strftime("%Y-%m", Transaction.date)
        rows = db.session.query(ym.label("ym"), func.sum(Transaction.amount_kd)).group_by("ym").order_by("ym").all()
        return jsonify([{"month": ym_val, "total_kd": float(total)} for ym_val, total in rows])

    @app.route("/api/transactions")
    def api_transactions():
        items = Transaction.query.order_by(Transaction.date.desc(), Transaction.id.desc()).all()
        return jsonify([t.to_dict() for t in items])


# -----------------------------------------------------------------------------
# CLI helpers (init DB, seed sample data)
# -----------------------------------------------------------------------------
def register_cli(app: Flask) -> None:
    @app.cli.command("init-db")
    def init_db_cmd():
        db.create_all()
        print("✅ Database initialized.")

    @app.cli.command("seed")
    def seed_cmd():
        defaults = ["Food", "Transport", "Rent", "Utilities", "Fun", "Misc", "Coffee", "Car Expenses"]
        for nm in defaults:
            if not Category.query.filter(func.lower(Category.name) == nm.lower()).first():
                db.session.add(Category(name=nm))
        db.session.commit()

        sample = [
            ("2025-06-01", "Food", "Shawarma lunch", "2.750"),
            ("2025-06-02", "Transport", "Taxi", "4.000"),
            ("2025-06-03", "Rent", "June rent", "300.000"),
            ("2025-06-05", "Utilities", "Electricity bill", "18.200"),
            ("2025-06-07", "Fun", "Cinema", "3.500"),
            ("2025-07-01", "Food", "Groceries", "12.350"),
            ("2025-07-02", "Transport", "Bus card", "5.000"),
            ("2025-07-03", "Rent", "July rent", "300.000"),
            ("2025-07-10", "Misc", "Gift", "9.900"),
            ("2025-08-01", "Food", "Breakfast", "1.800"),
            ("2025-08-03", "Utilities", "Water bill", "6.750"),
            ("2025-08-03", "Fun", "Bowling", "7.000"),
            ("2025-08-03", "Transport", "Fuel", "8.250"),
            ("2025-09-01", "Rent", "September rent", "300.000"),
            ("2025-09-05", "Food", "Dinner", "6.200"),
            ("2025-09-18", "Coffee", "Americano", "1.000"),
            ("2025-09-18", "Car Expenses", "Car gas", "5.350"),
        ]
        for d, cat, name, amt in sample:
            db.session.add(
                Transaction(
                    date=datetime.strptime(d, "%Y-%m-%d").date(),
                    category=cat,
                    name=name,
                    amount_kd=Decimal(amt),
                )
            )
        db.session.commit()
        print("✅ Seed data inserted.")


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
app = create_app()

if __name__ == "__main__":
    # For dev; production should use a WSGI server
    app.run(debug=True, host="127.0.0.1", port=5001)  # nosec B104