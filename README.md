# 🍷 Flex Pinot CLI

![Flex Pinot CLI Logo](source/logo.png)


A powerful Groovy-based command-line tool to create **Storage**, **Folder**, and **Inbox** resources in [Dalet Flex](https://www.dalet.com/products/flex/) from a CSV file.

> Pouring media resources with flavor.

---

## 🚀 Usage

```bash
groovy flex-pinot-cli.groovy [options] <path-to-csv>
```

## 🔧 Options

| Flag | Description |
|------|-------------|
| `-h`, `--help` | Show usage help |
| `-v`, `--version` | Print CLI version |
| `-u`, `--url <URL>` | Flex API base URL |
| `-U`, `--username <USERNAME>` | Flex username |
| `-P`, `--password <PASSWORD>` | Flex password (optional; will prompt securely if not provided) |
| `-d`, `--dry-run` | Parse CSV and validate without sending API calls |
| `-V`, `--verbose` | Print detailed logs for debugging |
| `-s`, `--skip-validation` | Bypass all pre-checks (not recommended in production) |
| `-f`, `--force` | Create resources even if they already exist |


## 🧪 Example

```bash
groovy flex-pinot-cli.groovy -u https://flex.example.com -U admin -V resources.csv
```

## 📄 CSV Format

### 📌 Required Columns

| Column | Required for | Description |
|--------|---------------|-------------|
| `Ref`  | All           | Unique name for the resource |
| `Type` | All           | One of: `Storage`, `Folder`, `Inbox` |

---

### 📀 For Storage

| Column | Required |
|--------|----------|
| `Protocol`, `Bucket`, `Hostname`, `Path`, `Key`, `Secret`, `Shard` | ✅ |

---

### 📂 For Folder

| Column | Required |
|--------|----------|
| `Link to` (refers to storage `Ref`) | ✅ |

---

### 📥 For Inbox

| Column | Required |
|--------|----------|
| `Link to` | ✅ |
| `WorkflowID`, `WorkflowOwner`, `InboxMetadata` | Optional, but validated if provided |


## ✅ Validation Logic

By default, the tool ensures:

- 🚫 No duplicate resource names (`/api/resources;name=Ref`)
- ✅ `WorkflowID` exists (`/api/workflowDefinitions/{id}`)
- ✅ `WorkflowOwner` exists (`/api/users/{id}`)
- ✅ `Metadata ID` exists (`/api/metadataDefinitions/{id}`)

---

### 🔍 Example API Calls (Performed Internally)

```bash
curl --location 'https://flex.example.com/api/resources;name=MyStorage'
curl --location 'https://flex.example.com/api/workflowDefinitions/415'
curl --location 'https://flex.example.com/api/users/61551'
curl --location 'https://flex.example.com/api/metadataDefinitions/7976130'
```

## ⚙️ Skipping or Forcing Validation

- Use `--skip-validation` to disable checks (faster, less safe)
- Use `--force` to override if a resource already exists

---

## 🔒 Security

- Password is **never shown** when prompted interactively
- Use `-P` only if needed for automation, and **beware of shell history**

---

## 🧠 Developer Notes

Next Steps:

- Hotfodler support
- CDN Support
- Pre Templated
- 

---

## 🧾 License

MIT License or internal usage license. Provided as-is.

---

## 🥂 Cheers

Created with ❤️ by the Dalet team.  
Happy automating your Flex resources 🍷
