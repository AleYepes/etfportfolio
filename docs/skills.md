## Grilling
---
name: grilling
description: Grill the user relentlessly about a plan, decision, or idea. Use when the user wants to stress-test their thinking, or uses any 'grill' trigger phrases.
---

Interview the user relentlessly until you reach a shared understanding. Map this as a design tree: every decision branches into the decisions that hang off it.

Work the tree in rounds. The frontier is every decision whose prerequisites are already settled: the questions you can ask _now_ without guessing at answers you haven't heard yet. Ask the whole frontier in one round: number each question and give your recommended answer. Then wait for the user's answers before the next round.

Format a round like so:

```
❓ Q1 - <question title>: <question body, might be multiple paragraphs, including multiple choices>
➡️ <your recommended answer>

❓ Q2 - <question title>: <question body, might be multiple paragraphs, including multiple choices>
➡️ <your recommended answer>
```

Each round the user answers reshapes the tree: settled decisions push the frontier outward and unblock questions that depended on them. Recompute the frontier and ask the next round. A question whose answer depends on another question still open in this round belongs to a _later_ round, not this one.

Finding _facts_ is your job, never the user's. When a frontier question needs a fact from the environment (filesystem, tools, etc.), dispatch a sub-agent to find it; don't ask the user for anything you could look up yourself. Don't block on it: a running exploration is an unsettled prerequisite, so only the questions downstream of it wait for the sub-agent to report; ask the rest of the frontier now. The _decisions_ are the user's: put each to them and wait.

The session is done when the frontier is empty: every branch of the design tree visited, nothing left silently assumed. Do not act on it until the user confirms you have reached a shared understanding.


## Personalized

Interview me until we reach a shared understanding.

Map this as a design tree, where every node represents a question and its corresponding decision. Every decision can branch into new questions that may be settled during the interview.

Explore the tree in rounds along its frontier. The frontier is the set of questions whose prerequisites have been settled clearly; the questions you can ask now without guessing at answers you haven't heard yet. Ask the whole frontier one round at a time. Each round, my answers will settle questions in the frontier, reshaping the tree and pushing the frontier outward, unblocking questions for the next round. For every round, enumerate the questions and give your recommended answer for each. Wait for my answers to settle questions before continuing to the next round.

Format a round like so:
```
❓ Q1 - <question title>: <question body, might be multiple paragraphs, including multiple choices>
➡️ <your recommended answer>

❓ Q2 - <question title>: <question body, might be multiple paragraphs, including multiple choices>
➡️ <your recommended answer>
```

Finding facts is your job. When a frontier question needs a fact from the environment (filesystem, tools, etc.), dispatch a sub-agent to find it or look it up yourself first. A question whose answer depends on another in the current round belongs to a following round, not the current round.

The session is done at my request or when the frontier is empty; when every branch of the design tree has been settled, and nothing is left silently assumed.




## Handoff
Write a handoff document summarising the current conversation so a fresh agent can continue the work.