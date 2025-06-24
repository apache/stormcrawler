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
- Note: This can only be done by an IPMC member. If you are a committer acting as a release manager, ask an IPMC member to add your key.

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

##### Put the artifacts to dist/dev

- Next, checkout the svn dist dev space from <https://dist.apache.org/repos/dist/dev/stormcrawler>
- Create a new folder `stormcrawler-x.y.z-RC1`.
- Add the `apache-stormcrawler-VERSION-source-release*` files from `stormcrawler/target/` to this folder.
  - `echo "  apache-stormcrawler-3.4.0-source-release.tar.gz" >> apache-stormcrawler-3.4.0-source-release.tar.gz.sha512`
  - `echo "  apache-stormcrawler-3.4.0-source-release.zip" >> apache-stormcrawler-3.4.0-source-release.zip.sha512`
- Ensure to add the file name to the `sha512` signature of the `apache-stormcrawler-VERSION-source-release*.zip.sha512` file as this is not automatically done via Maven.
- Commit the change set to the dist area (commit message "adding X.Y.Z-RC1"). Check that the files are present in <https://dist.apache.org/repos/dist/dev/stormcrawler/stormcrawler-x.y.z-RC1>

#### Check the Release Artifacts

Perform basic checks against the release binary:

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
Acknowledge the voting results on the mailing list in the VOTE thread by sending a mail.

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

### Merge the Release Branch

Merge the release branch into `main`.

### Commit Distribution to SVN

Move the distribution from dist/dev to dist/release via SVN

```bash
svn mv https://dist.apache.org/repos/dist/dev/stormcrawler/stormcrawler-x.y.z https://dist.apache.org/repos/dist/release/stormcrawler/stormcrawler-x.y.z -m "Release StormCrawler x.y.z"
```

This will make the release artifacts available on dist.apache.org and the artifacts will start replicating.

### Delete Old Release(s)

To reduce the load on the ASF mirrors, projects are required to delete old releases (see <https://www.apache.org/legal/release-policy.html#when-to-archive>).

Remove the old releases from SVN under <https://dist.apache.org/repos/dist/release/stormcrawler/>.

### Update the Website

- Merge the release branch to `main` to start the website deployment.
- Check, that the website is deployed successfully.
- Instruction on how to do so can be found on <https://github.com/apache/stormcrawler-site>

### Make the release on Github

- Remove the `draft` status from the release and select this release as the latest.

### Post-Release Steps

- Close the present release ticket
- Send an announcement email to <announce@apache.org> and <dev@stormcrawler.apache.org>.
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

A new release candidate can now be prepared. When complete, a new VOTE thread can be started as described in the steps above.
