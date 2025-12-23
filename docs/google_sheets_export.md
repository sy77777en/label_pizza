# Label Pizza – Google Sheets Export

This guide explains how to export annotation and review statistics from Label Pizza into Google Sheets. It automatically creates a **master sheet** and **individual user sheets** for annotators, reviewers, and admins.

You can use these sheets to track **progress, accuracy, payments, and feedback** without touching the database.

---

## What the script does

When you run the export:

1. Creates or updates a **master Google Sheet** with three tabs:

   * **Annotators**
   * **Reviewers**
   * **Meta‑Reviewers (Admins)**
2. Creates **individual Google Sheets for each user** with:

   * **Payment Tab** – where you can record payment info
   * **Feedback Tab** – where you can leave performance feedback
3. Updates **all statistics automatically** (completion %, accuracy %, last activity time).
4. **Manages permissions automatically**:

   * Admins get **edit** access
   * All other users get **view/comment‑only** access

You only need to **run the script**—all calculations and sharing are handled for you.

---

## Step‑by‑Step Setup

### 1. Prepare Google Cloud access

1. Go to [Google Cloud Console](https://console.cloud.google.com/).
2. **Create a new project** (or reuse an existing one).
3. **Enable the following APIs**:

   * **Google Sheets API**
   * **Google Drive API**
4. **Set up the OAuth Consent Screen**:

   * Choose **External**
   * Fill in basic info (app name + your email)
   * Save and continue
5. **Create OAuth 2.0 Credentials**:

   * Application type: **Desktop app**
   * Download the JSON file as **`credentials.json`**
   * Place it in your **project root**

> **Important:** Add both `credentials.json` and `google_sheets_token.json` to `.gitignore` for safety.

---

### 2. Create a Google Sheet

1. In Google Sheets, **create a blank spreadsheet**.
2. Copy its **Sheet ID** from the URL:

   ```
   https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit
   ```
3. This will be your **master sheet**.

---

### 3. First Run (Authorization)

Run the export script:

```bash
python label_pizza/google_sheets_export.py \
  --master-sheet-id <YOUR_SHEET_ID> \
  --database-url-name DBURL \
  --sheet-prefix <YOUR_PROJECT_NAME>
```

* On the **first run**, it will:

  1. Print an authorization URL.
  2. Open it in your browser.
  3. Show a **"This app isn't verified"** warning:

     * Click **Advanced → Go to \[App Name] (unsafe)**.
  4. Grant access to **Sheets and Drive**.
  5. Copy the **authorization code** from the URL and paste it back in the terminal.

A file called **`google_sheets_token.json`** will be saved automatically.

Future runs will **not require authorization again**.

---

### 4. Future Runs (Automatic)

After the first run, just call:

```bash
python label_pizza/google_sheets_export.py \
  --master-sheet-id <YOUR_SHEET_ID> \
  --database-url-name DBURL \
  --sheet-prefix <YOUR_PROJECT_NAME>
```

The script will:

* Update all stats and timestamps
* Keep your manual payment and feedback entries intact
* Refresh Google Sheet permissions automatically

---

## Command Line Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--master-sheet-id` | ✅ | - | Google Sheet ID from the URL |
| `--database-url-name` | ✅ | `DBURL` | Environment variable name for database URL |
| `--sheet-prefix` | ✅ | `Pizza` | Prefix for all individual sheet names |

### About `--sheet-prefix`

Individual user sheets are named using the format: `{prefix}-{UserName} {Role}`

For example, with `--sheet-prefix CameraBench`:
- `CameraBench-John Doe Annotator`
- `CameraBench-Jane Smith Reviewer`
- `CameraBench-Bob Admin Meta-Reviewer`

Choose a descriptive prefix for your project to keep sheets organized.

---

## Running as a Daily Job

To automatically export statistics every 24 hours, use the `run_daily_job.py` wrapper:

```bash
python run_daily_job.py --command "python label_pizza/google_sheets_export.py \
  --database-url-name <YOUR_DB_ENV_VAR> \
  --master-sheet-id <YOUR_SHEET_ID> \
  --sheet-prefix <YOUR_PROJECT_NAME>"
```

**Example:**
```bash
python run_daily_job.py --command "python label_pizza/google_sheets_export.py \
  --database-url-name MOTION_DBURL \
  --master-sheet-id 1R8jvlETlM_WPHbaP0YYGbahyeNz3vF7xn3rbK2Hde3c \
  --sheet-prefix CameraBench"
```

**Before running the daily job:**
1. Create a new Google Sheet and copy its ID from the URL
2. Add your database URL to `.env` (e.g., `MOTION_DBURL=postgresql://...`)
3. Choose a descriptive `--sheet-prefix` for your project

---

## Tips

* **Manual columns are preserved**
  Payment and feedback columns will **not be overwritten** by future exports.

* **Admins only edit**
  Only users marked as `admin` in the database will have full edit rights.

* **Hitting Google API rate limits?**
  If you have many users and run into rate limits, you can submit a support ticket to Google Cloud Console to request a quota increase for the Google Sheets API (typically 3–10x the default limit).

---

## Quick Checklist

Before running the export, confirm:

* [ ] `credentials.json` in project root
* [ ] Google Sheets + Drive APIs enabled
* [ ] Master sheet created and its ID copied
* [ ] `.env` contains a valid database URL (e.g., `DBURL` or `MOTION_DBURL`)
* [ ] `credentials.json` and `google_sheets_token.json` are in `.gitignore`
* [ ] Chosen a descriptive `--sheet-prefix` for your project

---

## What You'll Get

* **Master Sheet** with all annotators, reviewers, and admins
* **Individual Sheets** with:

  * Payment tab for tracking salary and bonuses
  * Feedback tab for performance notes
* **Automatic stats**: completion %, accuracy %, last activity time
* **Safe sharing**: admins edit, others view only

If you are a developer, you can find the code in [google_sheets_export.py](label_pizza/google_sheets_export.py) and full documentation in [google_sheets_export.md](label_pizza/google_sheets_export.md).

---

[← Back to start](start_here.md)