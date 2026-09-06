## Interview

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

I'm thinking we've stretched out this conversation a lot already, and would benefit from organizing everything we've discussed in an FRD-like document. This document should summarize our findings, conclusions and decisions clearly and comprehensively enough for a fresh, uninformed agent to catch up to speed and accurately understand the decisions we've settled and why.


If that clears the frontier, Let's draft a new FDR-like document that conveys all the decisions we've settled and why. The document should be clear and comprehensive enough so that a fresh agent (one without access to this conversation or the previous FDR) can accurately understand and implement all the specs. That may require straying from the conventional FDR structure to provide examples or anything else you think could be of value. The agent will receive the same preliminary context you did at the start of our conversation (Project overview, Project structure, and Architecture & coding principles) and they will access to read from and write to the repo directly.