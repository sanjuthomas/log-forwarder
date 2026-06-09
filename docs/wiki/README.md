# User guide (GitHub Wiki source)

Markdown in this directory is the **user-facing wiki** for log-forwarder. It focuses on running and configuring the forwarder, not on Go development.

## Read in the repo

Browse starting at [Home.md](Home.md).

## Publish to GitHub Wiki

GitHub Wikis use the same Markdown files but live in a separate git repository. After the wiki is enabled on the repository:

1. Create the first wiki page on GitHub (Wiki tab → **Create the first page**) if `git clone …/log-forwarder.wiki.git` fails with "Repository not found".

2. Clone the wiki repo:

   ```bash
   git clone https://github.com/sanjuthomas/log-forwarder.wiki.git
   cd log-forwarder.wiki
   ```

3. Copy pages from this directory (filenames must match — spaces become hyphens in GitHub wiki URLs):

   ```bash
   cp /path/to/log-forwarder/docs/wiki/*.md .
   git add .
   git commit -m "Add user guide wiki pages."
   git push
   ```

4. Open **https://github.com/sanjuthomas/log-forwarder/wiki**

### Page name mapping

| File | Wiki title |
|------|------------|
| `Home.md` | Home |
| `Installation-and-First-Run.md` | Installation and First Run |
| `How-It-Works.md` | How It Works |
| `Configuration-Guide.md` | Configuration Guide |
| `Choosing-a-Sink.md` | Choosing a Sink |
| `Spring-Boot-Logs.md` | Spring Boot Logs |
| `Watermarks-and-Restarts.md` | Watermarks and Restarts |
| `Example-Configs.md` | Example Configs |
| `Troubleshooting.md` | Troubleshooting |
| `_Sidebar.md` | Sidebar (navigation) |

Internal links use `[[Page Title]]` wiki syntax.

## Keeping wiki in sync

When user-facing behavior changes, update both:

- `docs/wiki/` (source of truth in git)
- GitHub Wiki (republish with the steps above)

Or automate with a CI job that pushes `docs/wiki/` to the wiki repo on release.
