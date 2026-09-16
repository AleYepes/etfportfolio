# Refactoring Initiative

## Project overview
The project you'll be working on runs factor-series analyses on ETF data. At a high level, it must:
- fetch ETF data from IBKR and supplementary data from other sources
- follow a medallion architecture via DuckDB schemas (`bronze`, `silver`, `gold`, and `cold_storage`)
- construct monthly LOCF panels for ETF fundamental metrics
- build weighted factor return series from each fundamental metric
- select a subset of factors as independent variables
- regress ETF returns on factor returns
- calculate efficient-frontier portfolios

## Project structure
The project's directory structure looks like this:
```
/etfportfolio
├── data/
├── etfportfolio/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── db.py
│   │   ├── logging.py
│   │   ├── progress.py
│   │   └── schema.sql
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── contracts.py
│   │   ├── details.py
│   │   ├── endpoints.py
│   │   ├── gateway.py
│   │   ├── landing.py
│   │   ├── pipeline.py
│   │   ├── prices.py
│   │   ├── products.py
│   │   ├── session.py
│   │   ├── snapshots.py
│   │   ├── themes.py
│   │   └── utils.py
│   └── observations/
│       ├── __init__.py
│       ├── cli.py
│       ├── extractors.py
│       ├── pipeline.py
│       └── utils.py
├── tests/
├── main.py
├── pyproject.toml
└── uv.lock
```
I've attached all the relevant files for you to review.

## Architecture & coding principles
1. **Simplicity**: Clear, simple, testable code. No hidden global state. No speculative abstractions. Do not build for hypothetical future needs.
2.  **Module Organization Guidelines**:
  - Used in only one script → stay declared in that script.
  - Shared across multiple scripts within a single directory/module (e.g., `ingestion/`, `observations/` `panel/`) → that directory's `utils.py`.
  - Shared across multiple directories/modules in `etfportfolio/` → `core/`.
  - Exceptions can be made if they improve the projects simplicity, clarity, concision, testability, etc.
3.  **Replacement Over Deprecation**: Prefer replacing old functionality cleanly rather than accumulating deprecated alternatives.

## Current objective
In this conversation, we'll focus on price ingestion concerns.
Your tasks are to:
1. Review the project's relevant architecture and functionality.
2. Review the following `ingestion.md` document and the implementation specs it describes.
3. Interview me until we reach a shared understanding of the right implementation plan.
Map the interview as a design tree, where every node represents a question and its corresponding decision. Every decision can branch into new questions that may be settled during the interview.
Explore the tree in rounds along its frontier. The frontier is the set of questions whose prerequisites have been settled clearly; the questions you can ask now without guessing at answers you haven't heard yet. Ask the whole frontier one round at a time. Each round, my answers will settle questions in the frontier, reshaping the tree and pushing the frontier outward, unblocking questions for the next round. For every round, enumerate the questions and give your recommended answer for each. Wait for my answers to settle questions before continuing to the next round.
Format a round like so:
```
❓ Q1 - <question title>: <question body, might be multiple paragraphs, including multiple choices>
➡️ <your recommended answer>
❓ Q2 - <question title>: <question body, might be multiple paragraphs, including multiple choices>
➡️ <your recommended answer>
```
Finding facts is your job. When a frontier question needs a fact from the environment (filesystem, tools, etc.), look it up yourself first. A question whose answer depends on another in the current round belongs to a following round, not the current round.
Resolving low-level concerns is also your job. This interview should focus on mid to high level design concerns.
The session is done at my request or when the frontier is empty; when every branch of the design tree has been settled and no mid to high level decisions are left silently assumed.