# zarr-java Project Charter

## Overview

[zarr-java](https://github.com/zarr-developers/zarr-java) is a Java implementation of the Zarr format. It is a Zarr Affiliated Software Project and operates under this charter, which adapts the [Governance for Zarr Affiliated Software Projects](https://github.com/zarr-developers/governance/blob/main/AFFILIATED_PROJECT_GOVERNANCE.md) to the needs of the project.

The project provides:

* an implementation of the Zarr format, supporting both Zarr v2 and Zarr v3, including the codecs, stores, and data types needed to read and write Zarr hierarchies from the JVM;
* an implementation of convention metadata layered on top of the Zarr format, most prominently [OME-Zarr](https://ngff.openmicroscopy.org/). The project is open to supporting further conventions where there is community interest and someone willing to maintain the code.

The project aims to track the Zarr Format Specification and the Zarr Extensions Registry closely, to remain interoperable with other implementations in the ecosystem, and to be a dependable dependency for downstream JVM software.

The project conducts its governance through open, asynchronous participation whenever practical, enabling broad participation across time zones and organizations. The primary venue for proposals, discussion, review, and decision records is GitHub.

## Roles and Responsibilities

### Core Developers

The Core Developers collectively steward zarr-java. They have administrative rights on the project's repositories and are responsible for:

* maintaining the zarr-java codebase, documentation, and project infrastructure;
* reviewing and merging pull requests;
* preparing and publishing releases;
* tracking changes to the Zarr Format Specification, and implementing them;
* tracking changes to the Zarr Extensions Registry, and implementing them on a case-by-case basis;
* deciding which conventions the project supports and to what extent;
* considering the impact of decisions on downstream users and on interoperability with other Zarr implementations;
* coordinating with the wider Zarr community.

The Core Developers have administrative privileges over the following resources:

* the [`@zarr-developers/zarr-java`](https://github.com/zarr-developers/zarr-java) GitHub repository;
* the `dev.zarr` namespace on Maven Central under which the project's artifacts are released, and the signing keys and credentials used to publish them;
* the [`@zarr-developers/java-core-devs`](https://github.com/orgs/zarr-developers/teams/java-core-devs) GitHub team.

Membership is merit-based and recognizes sustained, quality contribution to the project. Contributions may include code, code review, documentation, release engineering, interoperability testing, issue triage, user support, or other activities that advance the goals of the project.

Any Core Developer may nominate a new Core Developer. New Core Developers are admitted by a **majority vote** of the existing Core Developers. Core Developers who become inactive may be removed by a **majority vote** of the existing Core Developers, with the Core Developer in question recused. Core Developers may resign at any time.

Current Core Developers are:

- [Sébastien Besson](https://github.com/sbesson)
- [Konstantin Bobenko](https://github.com/konstibob)
- [Melissa Linkert](https://github.com/melissalinkert)
- [Josh Moore](https://github.com/joshmoore)
- [Norman Rzepka](https://github.com/normanrz)

### Envoy to the Zarr Format Working Group

The Envoy is a Core Developer who represents zarr-java in the [Zarr Format Working Group](https://github.com/zarr-developers/governance/blob/main/zarr-format-working-group/README.md) (ZFWG). The Envoy is a member of the ZFWG and therefore requires a nomination and approval by the ZFWG according to that group's charter; the Core Developers cannot appoint an Envoy unilaterally.

The Envoy:

* carries the implementation experience, constraints, and concerns of zarr-java into the deliberations of the ZFWG;
* keeps the Core Developers informed about proposals, decisions, and releases of the Zarr Format Specification and the Zarr Extensions Registry;
* helps the project plan for upcoming specification changes.

The Envoy holds the same rights within the project as any other Core Developer, and no additional authority. Should the Envoy cease to be a Core Developer or cease to be a member of the ZFWG, the role becomes vacant.

Current Envoy is:

- [Norman Rzepka](https://github.com/normanrz)

### Community

Everyone else in the community is welcome to participate in the development of zarr-java by reporting bugs, proposing ideas, contributing code or documentation, reviewing pull requests, sharing usage and interoperability experience, and supporting other users.

Contributions from the Community are made through the same public process as those from Core Developers. Participation must follow the Zarr Project Code of Conduct.

## Decision Process

The Core Developers seek to make decisions through informed consensus. If consensus cannot be achieved, a simple majority of the Core Developers decides, except where specified differently below.

Decisions are made and recorded asynchronously on GitHub wherever practical. Meetings and other communication channels may be used to exchange ideas or resolve questions, but decisions should be recorded through the process described here, so that all Core Developers have a reasonable opportunity to participate.

### Merging Pull Requests

Changes to the code and documentation are made through pull requests. Anyone from the Community or the Core Developers may open a pull request.

Pull requests submitted by an external contributor should be reviewed and approved by at least one Core Developer before being merged. Ideally, pull requests submitted by a Core Developer should be reviewed and approved by at least one other Core Developer before being merged.

Pull requests should not be merged until all CI checks have passed (GitHub Actions) against code that has had the latest `main` merged in.

Core Developers are expected to use their judgement about how much review a change warrants, and to allow time for others to comment on changes that are large, that affect public API, or that affect on-disk compatibility.

There is no obligation for Core Developers to review pull requests and Core Developers may close stale pull requests without further review. Core Developers may also update pull requests to resolve their objections without waiting on the pull request author to implement them.

Pull requests should not be merged while objections from Core Developers stand. Any Core Developer may unilaterally call a Formal Vote at any time to resolve stalled discussions on a pull request.

### Self-Merging Pull Requests

The default is that a pull request opened by a Core Developer is approved by another Core Developer before it is merged. We trust Core Developers to use their judgement, though, and we would rather bias toward action than make routine changes wait on review they do not really need.

A Core Developer may therefore merge their own pull request when they judge the change to be low-risk, provided CI passes against code that has the latest `main` merged in and other Core Developers have had a fair chance to weigh in. As a rule of thumb, leave the pull request open for a few days before self-merging, unless it is genuinely trivial or time-sensitive. Pinging another Core Developer for awareness is advisable even when no formal review is requested.

Some changes warrant more caution, and a second reviewer is worth seeking even when self-merging would be permitted: changes to the public API, anything touching the Zarr format or on-disk compatibility, support for a new convention, and performance-sensitive code. These are the most expensive to get wrong and the hardest to reverse. Reverts, by contrast, are cheap: if a self-merged change turns out to be a mistake, reverting it is itself a low-risk change that any Core Developer may make, and the reworked version can go through normal review. When something recently merged is actively causing harm — a broken `main`, a release blocker, or data corruption — fix it quickly and request review after the fact.

This policy exists to lower the cost of routine work and to help newer Core Developers grow comfortable merging changes. It is not a license to merge past an unresolved objection: if another Core Developer asks to review a change, give them that chance.

### Releases

Releases are decided by a **majority vote of the Core Developers**, applying lazy consensus: a Core Developer states the intent to make a release in a public GitHub issue or pull request, and if no Core Developer objects within three business days, the release may proceed. If there are objections, the release requires an explicit majority of the Core Developers, or a Formal Vote may be called.

Version numbers adhere to [Semantic Versioning](https://semver.org/). In particular, breaking changes to the public API of the library require a major version increment, and the Core Developers should document such changes in the release notes.

Releases are published from the project's repository and to the usual distribution channels for JVM libraries.

#### Emergency Releases

Releasing a fix for a critical bug or a security vulnerability must always be possible without delay. Any Core Developer may make an emergency release immediately, without waiting for the lazy consensus period, provided the change is limited to the fix and the release adheres to Semantic Versioning.

The Core Developer making an emergency release announces it in a public GitHub issue at the time of the release, stating what was fixed and why the release could not wait. The release is then reviewed by the Core Developers after the fact; if the Core Developers disagree with the decision, the remedy is a follow-up release, not a retraction of the published artifact.

Security vulnerabilities may be discussed privately among the Core Developers until a fix is available, and should be disclosed publicly once users have a released version to upgrade to.

### Other Decisions

All other decisions — including implementing support for a new convention, adding or dropping a dependency, changing project infrastructure, or setting technical direction — are made by **lazy consensus**: a Core Developer states their intent in a public GitHub issue or pull request, and if no Core Developer objects within three business days, they may proceed. Lazy consensus is the default for the day-to-day operation of the project.

If an objection is raised, the proposal is discussed until consensus is reached. If consensus appears unreachable, any Core Developer may call a Formal Vote.

### Governance Decisions

The following decisions require a Formal Vote:

* admitting a new Core Developer;
* removing a Core Developer;
* nominating a Core Developer to serve as Envoy to the ZFWG (the appointment itself additionally requires approval by the ZFWG); and
* adopting amendments to this charter.

The Zarr Steering Council reserves the right to revoke the Affiliated Project status, if amendments make the charter inconsistent with Zarr Project Governance.

## Formal Vote

- A motion can be proposed by any Core Developer in a GitHub issue (by tagging the Core Developers) or by email, if it needs to be held in private.
- Core Developers vote on the motion by a GitHub comment or by replying to the email.
- We allow two weeks for voting after a motion is proposed.
- If after two weeks a majority of Core Developers has voted and all votes agree, the majority vote becomes the decision.
- If there are any disagreements within the initial two week voting period, a further two weeks will be allowed to try and reach consensus.
- If after 4 weeks there is no consensus, the majority vote becomes the decision.

If a Core Developer believes the decision process is stalled at any time, they may unilaterally call for a Formal Vote.

## Code of Conduct

The project operates under the [Zarr Project Code of Conduct](https://github.com/zarr-developers/governance/blob/main/CODE_OF_CONDUCT.md). All participants are expected to contribute respectfully and constructively.
