# Guide to release Apache StormCrawler

## Release Preparation

- Select a release manager on the dev mailing list. A release manager should be a committer and should preferably switch between releases to have a transfer in knowledge.
- Create an issue for a new release in <https://github.com/apache/stormcrawler/issues>
- Review all [issues](https://github.com/apache/stormcrawler/issues) associated with the release. All issues should be resolved and closed.
- Any issues assigned to the release that are not complete should be assigned to the next release. Any critical or blocker issues should be resolved on the mailing list. Discuss any issues that you are unsure of on the mailing list.

## Steps for the Release Manager

The following steps need only to be performed once.

- Make sure you have your PGP fingerprint added into <https://id.apache.org/>
- Make sure you have your PGP keys password.
- Add your PGP key to the [KEYS](https://dist.apache.org/repos/dist/release/stormcrawler/KEYS) file.

Examples of adding your key to this file:

```
pgp -kxa <your name> and append it to this file.
(pgpk -ll <your name> && pgpk -xa <your name>) >> this file.
(gpg --list-sigs <your name>
&& gpg --armor --export <your name>) >> this file.
```

- In a local temp folder, svn checkout the StormCrawler artifacts and update the KEYS file
- Note: This can only be done by an PMC member. If you are a committer acting as a release manager, ask an PMC member to add your key.

```
svn co https://dist.apache.org/repos/dist/release/stormcrawler
svn commit -m "Added Key for <name>" KEYS
```

- Create a Maven `settings.xml` in `~/.m2` to publish to `repository.apache.org`.
- An example configuration can look like:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.1.0 http://maven.apache.org/xsd/settings-1.1.0.xsd"
          xmlns="http://maven.apache.org/SETTINGS/1.1.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <servers>
        <server>
            <id>apache.snapshots.https</id>
            <username>your-asf-ldap</username>
            <password>your-asf-ldap-password</password>
        </server>
        <server>
            <id>apache.releases.https</id>
            <username>your-asf-ldap</username>
            <password>your-asf-ldap-password</password>
        </server>
        <server>
            <id>apache.dist.https</id>
            <username>your-asf-ldap</username>
            <password>your-asf-ldap-password</password>
        </server>
    </servers>
    <profiles>
        <profile>
            <id>apache-gpg</id>
            <properties>
                <gpg.keyname>your-gpg-code-signing-key-fingerprint</gpg.keyname>
            </properties>
            <repositories>
                <repository>
                    <id>apache.dist.https</id>
                    <url>https://dist.apache.org/repos/dist</url>
                </repository>
            </repositories>
        </profile>
    </profiles>
</settings>
```

- Note: You can also encrypt related secrets via [Maven build-in methods](https://maven.apache.org/guides/mini/guide-encryption.html).

- In case you are running on a headless system, it might be necessary to set the following export before starting with the release preparation.

```bash
export GPG_TTY=$(tty) 
```

## Using Apache Trusted Releases (ATR)

[ATR](https://releases.apache.org/) is the ASF platform for staging, voting on and publishing releases. It replaces the `dist/dev`
staging area, sends the vote and announcement emails on your behalf, and publishes the approved files to `dist/release`. It is currently
in beta. ATR does **not** build or sign the artifacts and does **not** publish to Maven Central: building, signing and the Nexus staging
repository work as described in the [Release Steps](#release-steps).

> **Pick one path for each release, ATR or SVN, and never mix them.** The two paths diverge when the release candidate is staged, at the
> vote email, and again when publishing and announcing after the vote. Steps marked **ATR** and **SVN** below are alternatives: follow only
> the variant of the path you chose. Mixing them, for example staging in `dist/dev` and also starting the release in ATR, leaves two diverging
> copies of the release candidate. Inside ATR you are free to switch between the website and the command line.

Every ATR step can be done either on the **website** (<https://releases.apache.org>, log in with your ASF account) or from the
**command line** with the `atr` client. Voting still happens on <dev@stormcrawler.apache.org>, and only votes from the StormCrawler PMC are binding.

### One-time setup

1. Register for the ASF multi-factor authentication at <https://mfa.apache.org/>.
2. You must be a PMC member, or be designated as a release manager by a PMC member on the StormCrawler committee page in ATR.
3. Make sure ATR knows your OpenPGP signing key. By default ATR imports the StormCrawler KEYS file from SVN, so adding your key to KEYS as described above
   is enough. You can also add the key at <https://releases.apache.org/keys>. New keys must be RSA of at least 4096 bits, or Ed25519 / ECDSA (P-256 or stronger); DSA keys are rejected.
4. Get the credentials for the way you upload files:
   - website: nothing more than your ASF login;
   - `rsync`: add an SSH **public** key at <https://releases.apache.org/keys>. This is a different key from your OpenPGP key. You connect as your ASF UID on port 2222;
   - command line: create a personal access token at <https://releases.apache.org/tokens> (it expires after 180 days).
5. To use the command line, install the client (it needs [uv](https://docs.astral.sh/uv/getting-started/installation/)) and sign in:

   ```bash
   uv tool install "apache-trusted-releases @ git+https://github.com/apache/tooling-releases-client"
   atr set asf.uid <your-asf-uid>
   atr set tokens.pat
   atr jwt refresh
   ```

   `atr set tokens.pat` asks for the token without echoing it. Run `atr --help` or `atr docs` for the full command reference. The client is not yet stable, so do not use it in unattended scripts.
6. Log in on the website and check that the StormCrawler project is listed. The commands below use `stormcrawler` as its project key; check it on the project page.
7. In the project settings, set **Default download path suffix** to `stormcrawler-{{VERSION}}`. It pre-fills the publish path. With the field empty ATR publishes
   the files straight into `dist/release/stormcrawler/`, and not into `stormcrawler-x.y.z/` as every earlier release did.
8. ATR's default vote and announcement emails do not contain everything our emails had (tag, Nexus staging repository, website preview, release notes,
   list of changes). Either adjust the templates once in the project settings (**Vote** and **Announce** tabs; the available `{{VARIABLES}}` are listed there),
   or edit the email manually before you send it, using the templates in [Create a VOTE Thread](#create-a-vote-thread) and [Post-Release Steps](#post-release-steps).

### ATR operations at a glance

| Step | Website | Command line (`x.y.z` is the release version) |
|---|---|---|
| Start the release | **Draft → Candidates → Start a new release** | `atr release start stormcrawler x.y.z` |
| Upload the files | Compose page → **Upload files** (browser, `svn` or `rsync`) | `atr rsync stormcrawler x.y.z ./candidate/` or `atr upload ...` |
| Review the checks | Compose page | `atr check wait\|status\|blockers\|concerns stormcrawler x.y.z` |
| Start the vote | Compose page → **Start voting** | `atr vote start stormcrawler x.y.z -m dev@stormcrawler.apache.org --subject ... --body-file vote.txt` |
| Read the tally | Vote page | `atr vote tabulate stormcrawler x.y.z` |
| Resolve the vote | Vote page → **Resolve vote** (the only way for email votes) | not available for email votes |
| Record the Maven Central release | Finish page → **Verify a third-party distribution** | `atr distribution record stormcrawler x.y.z MAVEN org.apache.stormcrawler <artifactId> x.y.z` |
| Publish to `dist/release` | Finish page → **Publish to ASF Distribution Area** | add `--auto-publish` to `vote start` (PMC members only) |
| Announce | Finish page → **Announce** | `atr announce stormcrawler x.y.z -m announce@apache.org --body-file announce.txt` |

### What ATR checks

ATR checks the source archives with its own rules, not with our build configuration: the ASF license header word for word in source files,
`This product includes software developed at The Apache Software Foundation` in `NOTICE`, and Apache RAT with its default settings.
RAT reads exclusions only from the [`.rat-excludes`](.rat-excludes) file at the top of the archive, not from the RAT configuration in `pom.xml`.
When you add an exclusion to the `rat` profile in `pom.xml`, add the same pattern to `.rat-excludes`.

## Release Steps

- Checkout the Apache StormCrawler main branch: `git clone git@github.com:apache/stormcrawler.git`
- Execute a complete test: `mvn test` 
  - Ensure to have a working Docker environment on your release machine. Otherwise, coverage computation goes wrong and the build will fail.
- Check the current results of the last GitHub action runs.
- Upgrade to the latest version of StormCrawler in the README files
- Do a trial build: `mvn package -Papache-release,apache-gpg`
- Switch to a new branch with a format like **rel-stormcrawler-x.y.z-RC?**
- Prepare the release: `mvn release:prepare -Papache-release,apache-gpg` Answer the questions appropriately. The tag name format should be *stormcrawler-x.y.z*. **DO NOT INCLUDE `-rcX` in the tag**
  This command creates and pushes two new commits to the repository to reflect the version changes. It also tags the release and pushes the branch.
- Check the result of the GitHub action runs.

### Successful Maven Release Preparation

#### Perform the Release

- Perform the release: `mvn release:perform -Papache-gpg`
- This creates a staged repository at <https://repository.apache.org/#stagingRepositories>
- Check the staged repository and if all looks well, close the staging repository but do *not* promote or release it at this time.
- The build results are in `stormcrawler/target/`. Do not modify or delete these files.

##### Stage the release candidate

Take the `apache-stormcrawler-VERSION-source-release*` files (`.tar.gz`, `.zip` and their `.asc` and `.sha512`) from `stormcrawler/target/`.
Add the file name to the `sha512` files, as this is not done by Maven:

```bash
echo "  apache-stormcrawler-x.y.z-source-release.tar.gz" >> apache-stormcrawler-x.y.z-source-release.tar.gz.sha512
echo "  apache-stormcrawler-x.y.z-source-release.zip" >> apache-stormcrawler-x.y.z-source-release.zip.sha512
```

- **ATR:** start the release, on the website (**Draft → Candidates → Start a new release**) or with `atr release start stormcrawler x.y.z`.
  A failed vote returns the release to the Compose phase, so the version stays the same and the RC number only appears in the vote email.
  Put the six files in a local directory such as `./candidate/` and upload them on the website (Compose page → **Upload files**) or from the command line:

  ```bash
  atr rsync stormcrawler x.y.z ./candidate/
  ```

  Then wait for the checks on the Compose page, or run:

  ```bash
  atr check wait stormcrawler x.y.z --timeout 300
  atr check status stormcrawler x.y.z --verbose
  atr check blockers stormcrawler x.y.z --members
  atr check concerns stormcrawler x.y.z --members
  ```

  Fix every blocker (re-sign, re-checksum and re-upload the affected files). Read every concern: you acknowledge the remaining ones when you start the vote.
  ATR also asks for the full 40-character commit the sources were built from: `git rev-parse stormcrawler-x.y.z^{commit}`.
- **SVN:** checkout the svn dist dev space from <https://dist.apache.org/repos/dist/dev/stormcrawler>, create a new folder `stormcrawler-x.y.z-RC1` and add the files to it.
  Commit the change set to the dist area (commit message "adding X.Y.Z-RC1"). Check that the files are present in <https://dist.apache.org/repos/dist/dev/stormcrawler/stormcrawler-x.y.z-RC1>

#### Check the Release Artifacts

Perform basic checks against the release binary. With **ATR**, the Compose page already shows the results of the signature, checksum, license and RAT checks.

- Check signature of generated source artifacts. This can be done like that:

```bash
#!/bin/bash

mkdir /tmp/test
cd /tmp/test
curl -s -O https://dist.apache.org/repos/dist/release/stormcrawler/KEYS
curl -s -O https://dist.apache.org/repos/dist/dev/stormcrawler/stormcrawler-x.y.z-RC1/apache-stormcrawler-x.y.z-source-release.tar.gz
curl -s -O https://dist.apache.org/repos/dist/dev/stormcrawler/stormcrawler-x.y.z-RC1/apache-stormcrawler-x.y.z-source-release.tar.gz.asc

echo "
list keys
"
gpg --homedir . --list-keys

echo "
import KEYS file
"
gpg --homedir . --import KEYS

echo "
verify signature
"
gpg --homedir . --output  apache-stormcrawler-x.y.z-source-release.tar.gz --decrypt apache-stormcrawler-x.y.z-source-release.tar.gz.asc
```

- Check presence and appropriateness of `LICENSE`, `NOTICE`, and `README.md` files.

### Prepare the Website

- Create a separate branch for the release.
- Run a global replace of the old version with the new version.
- Prepare a preview via the staging environment of the website.
- Ensure the website is updated on <https://stormcrawler.staged.apache.org>
- Note: Instruction on how to do so can be found on <https://github.com/apache/stormcrawler-site>

### Create a draft release on Github

- Create a new Draft Release -- on <https://github.com/apache/stormcrawler/releases>, click `Draft a new release` and select the `stormcrawler-X.Y.Z` tag.
- Click the `Generate Release Notes` (**MAKE SURE TO SELECT THE CORRECT PREVIOUS RELEASE AS THE BASE**). Copy and paste the Disclaimer and Release Summary from the previous release and update the Release Summary as appropriate.
- Click the `Set as pre-release` button.
- Click `Publish release`. The release should not have `*-rc1` in its title, e.g.: `https://github.com/apache/stormcrawler/releases/tag/stormcrawler-3.4.0`

#### Create a VOTE Thread

The VOTE process is two-fold:

- Create a vote on <dev@stormcrawler.apache.org>
  - **ATR:** start the vote on the website (Compose page → **Start voting**) or from the command line. ATR sends the email to the list you choose, in your name.
    The website pre-fills the email from the project's vote template, with the links to the candidate, the file manifest and the KEYS file. If the template has not been
    adjusted (see [One-time setup](#one-time-setup)), add the tag, the Nexus staging repository, the website preview and the release notes from the template below
    manually. Review the text, and send.
    From the command line, write the whole email body yourself and pass the subject line separately:

    ```bash
    atr vote start stormcrawler x.y.z -m dev@stormcrawler.apache.org -d 72 --subject "[VOTE] Apache StormCrawler x.y.z Release Candidate" --body-file vote.txt
    ```

    Add `--concerns-noted <keys>` to acknowledge the remaining concerns. `--auto-publish` (PMC members only) publishes the files as soon as the vote is resolved;
    leave it out if you want to control when that happens.
  - **SVN:** send the email yourself, using the template below.

- Be sure to replace all values in `[]` with the appropriate values.

```bash
Message Subject: [VOTE] Apache StormCrawler [version] Release Candidate

----
Hi folks,

I have posted a [Nth] release candidate for the Apache StormCrawler[version] release and it is ready for testing.

<Add a summary to highlight notable changes>

Thank you to everyone who contributed to this release, including all of our users and the people who submitted bug reports,
contributed code or documentation enhancements.

The release was made using the Apache StormCrawler release process, documented here:
https://github.com/apache/stormcrawler/blob/main/RELEASING.md

Source:

https://dist.apache.org/repos/dist/dev/stormcrawler/stormcrawler-x.y.z-RC1

Tag:

https://github.com/apache/stormcrawler/releases/tag/stormcrawler-x.y.z

Commit Hash:

Add the commit hash of the release

Maven Repo:

https://repository.apache.org/content/repositories/orgapachestormcrawler-XXXX

<repositories>
<repository>
<id>stormcrawler-y.x.z-rc1</id>
<name>Testing StormCrawler x.y.z release candidate</name>
<url>
https://repository.apache.org/content/repositories/orgapachestormcrawler-XXXX
</url>
</repository>
</repositories>

Preview of website:

https://stormcrawler.staged.apache.org/download/index.html

Release notes:

<Add link to the GitHub release notes>

Reminder: The up-2-date KEYS file for signature verification can be
found here: https://downloads.apache.org/stormcrawler/KEYS

Please vote on releasing these packages as Apache StormCrawler x.y.z 
The vote is open for at least the next 72 hours.

Only votes from the StormCrawler PMC are binding, but everyone is welcome to check the release candidate and vote.
The vote passes if at least three binding +1 votes are cast.

Please VOTE

[+1] go ship it
[+0] meh, don't care
[-1] stop, there is a ${showstopper}

Thanks!

<Your-Name>
```

## After a Successful Vote

The vote is successful if at least 3 *+1* votes are received from PMC members after a minimum of 72 hours of sending the vote email.

- **ATR:** check ATR's tally against the vote thread, then resolve the vote on the vote page (**Resolve vote**; this is the only way to resolve an email vote).
  `atr vote tabulate stormcrawler x.y.z` shows the tally from the command line. ATR emails the outcome to the list, and a passing vote moves the release to the Finish phase.
- **SVN:** acknowledge the voting results on the mailing list in the VOTE thread by sending a mail.

```bash
Message Subject: [RESULT] [VOTE] Apache StormCrawler [version]

Hi folks,

this vote passes with the following +1 being cast:

- PMC Name Y (binding)
- PMC Name X (binding)
- User Name Z
- PMC Name YY (binding)
- User Name ZZ

Thanks to all voters. I'll proceed with the steps.

<Your-Name>
```

### Release Nexus Staging Repository

Release the staging repository. This will make the artifacts available in the Maven Central repository.
To do this go to the <https://repository.apache.org[repository> server], log in, go to the staging area and release the staging repository linked to this release

**ATR:** then record the Maven Central release in ATR, on the Finish page (**Verify a third-party distribution**) or from the command line, once per artifact:

```bash
atr distribution record stormcrawler x.y.z MAVEN org.apache.stormcrawler <artifactId> x.y.z
```

ATR can require this before it lets you announce.

### Merge the Release Branch

Merge the release branch into `main`.

### Publish the Distribution

- **ATR:** on the Finish page use **Publish to ASF Distribution Area**, and set the download path suffix to `stormcrawler-x.y.z` so the files end up where they always have.
  The field is pre-filled if you did the [One-time setup](#one-time-setup); check it before you publish. If it is empty the files land directly in `dist/release/stormcrawler/`, and you
  have to move them into `stormcrawler-x.y.z/` with one `svn mv` commit afterwards.
  If you started the vote with `--auto-publish`, ATR has already done this when the vote was resolved.
- **SVN:** move the distribution from dist/dev to dist/release:

  ```bash
  svn mv https://dist.apache.org/repos/dist/dev/stormcrawler/stormcrawler-x.y.z-RC1 https://dist.apache.org/repos/dist/release/stormcrawler/stormcrawler-x.y.z -m "Release StormCrawler x.y.z"
  ```

This will make the release artifacts available on dist.apache.org and the artifacts will start replicating.

### Delete Old Release(s)

To reduce the load on the ASF mirrors, projects are required to delete old releases (see <https://www.apache.org/legal/release-policy.html#when-to-archive>).

Remove the old releases from SVN under <https://dist.apache.org/repos/dist/release/stormcrawler/>. This is also needed with ATR.

### Update the Website

- Merge the release branch to `main` to start the website deployment.
- Check, that the website is deployed successfully.
- Instruction on how to do so can be found on <https://github.com/apache/stormcrawler-site>

### Make the release on Github

- Remove the `draft` status from the release and select this release as the latest.

### Post-Release Steps

- Close the present release ticket
- Send an announcement email to <announce@apache.org> and <dev@stormcrawler.apache.org>.
  - **ATR:** on the Finish page use **Announce**, or run `atr announce stormcrawler x.y.z -m announce@apache.org --body-file announce.txt`, with the text below as the body.
    ATR sends it in your name.
  - **SVN:** send it yourself.
- Make sure the mail is **plain-text only**.
- It needs to be sent from your **@apache.org** email address or the email will bounce from the announce list.

```bash
Title: [ANNOUNCE] Apache StormCrawler <version> released
TO: announce@apache.org, dev@stormcrawler.apache.org
----

Message body:

----
The Apache StormCrawler team is pleased to announce the release of version <version> of Apache StormCrawler. 
StormCrawler is a collection of resources for building low-latency, customisable and scalable web crawlers on Apache Storm.

Apache StormCrawler <version> source distributions is available for download from our download page: https://stormcrawler.apache.org/download/index.html
Apache StormCrawler is distributed by Maven Central as well. 

Changes in this version:

**  TODO UPDATE THIS >>
- change 1
- change 2
<<  TODO UPDATE THIS **

For a complete list of fixed bugs and improvements please see the release notes on GitHub.

The Apache StormCrawler Team

```

## After an Unsuccessful Vote

The release vote may fail due to an issue discovered in the release candidate. If the vote fails the release should be canceled by:

- Sending an email to <dev@stormcrawler.apache.org> on the VOTE thread notifying of the vote's cancellation.
- Dropping the staging repository at <https://repository.apache.org/>.
- Renaming the `stormcrawler-x.y.x` tag to `stormcrawler-x.y.z-RC1`.
- **ATR:** resolving the vote as failed returns the release to the Compose phase. Upload the files of the next release candidate to the same release.

A new release candidate can now be prepared. When complete, a new VOTE thread can be started as described in the steps above.
