# Workspace Management Guide

## Managing Your Workspace Folder

Once you understand how the workspace folder works, you have two main options for managing your workspace data:

#### Option 1: Simple workspace usage (easiest to get started)

Use the empty `workspace/` folder directly in this repo:

```bash
# Use the existing workspace folder
python sync_from_folder.py --folder-path ./workspace
```

**Important:** Add your workspace folder to `.gitignore` to avoid leaking sensitive data:

```bash
echo "workspace/" >> .gitignore
```

⚠️ **Note:** Since this label_pizza repo is public, any fork must also be public. This option provides no version control for your workspace data.

#### Option 2: Private workspace with version control

**Setup your private workspace repo:**

```bash
# 1. Create a separate private repo for workspace data
# (Do this on GitHub: Create New Repository → Private)

# 2. Clone your private workspace repo
git clone https://github.com/yourusername/your-private-workspace.git
cd your-private-workspace

# 3. Create your workspace structure and add your JSON files
mkdir my_project_workspace
# Add: videos.json, users.json, question_groups/, schemas.json, 
#      projects.json, assignments.json, verify.py, etc.

# 4. Commit to your private repo
git add .
git commit -m "Initial workspace setup"
git push origin main
```

**Access your workspace (choose one):**

**Option 2a: Direct path (recommended)**
```bash
cd /path/to/label_pizza
python sync_from_folder.py --folder-path ../your-private-workspace/my_project_workspace
```

**Option 2b: Symlink (for convenience)**
```bash
cd /path/to/label_pizza
ln -s ../your-private-workspace/my_project_workspace ./my_workspace
echo "my_workspace" >> .git/info/exclude # Important: Add to .git/info/exclude to avoid committing the symlink
python sync_from_folder.py --folder-path ./my_workspace
```

This approach gives you:
- ✅ Full version control of your workspace data
- ✅ Private storage of sensitive information  
- ✅ Easy collaboration with team members
- ✅ Backup and history of your labeling configurations

---

[← Back to start](start_here.md) | [Next → Custom Question](custom_display.md)