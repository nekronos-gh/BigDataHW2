# AI Usage Disclosure

## AI Usage by Team Member

### Eduardo Rodríguez Sánchez

**AI Platform Used:** Gemini 3.1 Pro Preview (via Google AI Studio - free tier for students)

**Development Environment:**

- Editor: Neovim with opencode plugin integration
- LLM Interface: TUI-based chat in left console with full repository context

**Workflow & Prompting Strategy:**

1. **Small Task Delegation:** The LLM handles atomic tasks with full codebase context:
   - README file generation
   - Code comments and documentation
   - Boilerplate implementations
   - Regex pattern development and validation
   - Localized code fixes

2. **Self-Handled Tasks:** Architecture-level decisions and problem-solving strategies are made manually to ensure:
   - Proper understanding of distributed computing concepts
   - Sofware architecure
   - Performance considerations

3. **Documentation Research:** Perplexity AI (LLM-powered search) is used for looking up:
   - Apache Hadoop/Spark API documentation
   - Spark DataFrame operations
   - Technical specifications of frameworks used

**Key Prompting Steps:**

- Context is provided via opencode's repository-wide context injection
- Specific file paths and line numbers are referenced for code modifications
- Clear task boundaries are set for each delegation
- Results are reviewed before integration

---

### Michalina Miszkiewicz

**AI Platform Used:** ChatGPT, Microsoft Copilot

**Development Environment:**

- Editor:Visual Studio Code and terminal-based editing (`nano`/ `vim`)
- LLM Interface: browser-based ChatGPT and Microsoft Copilot

1. **Task delegation:** The LLM handled tasks with the full exercise context:
   - Help with improving the top-100 computation
   - Help with the debugging and optimization process
   - Checking and adapting grammar in the documentation
   - Adding better, more readable comments in the code

2. **Self-Handled Tasks:**
   - Designing the Spark pipeline for tokenization, counting, pair generation, and final output extraction
   - Rebuilding the jar, running and validating the Spark jobs on IRIS, and verifying the correctness of intermediate and final outputs
   - Executing the full scalability experiment from `AA` to `AK`, collecting runtime and hardware measurements, and preparing the base documentation

3. **AI-assisted tasks:**
   - Help with understanding the tasks.

**Key Prompting Steps:**

1. Interpret the exercise requirements for Spark based on Problem 1
2. Paste the instructions to the LLM and ask for clarifications.
3. Create base solutions and ask for help if problems show up
4. Review and test proposed changes
5. Ask for improviment of specific functions
6. Rewiew the and adapt the solutions
7. Ask for adding comment to make code more readable
8. Ask for fixes in prepard documentation

---

### Henrik Klasen

**AI Platform used:** Github Copilot (using student package on Github)

**Development Environment:**

- Editor: Visual Studio Code for major changes (Writing the Java classes), vim/nano for minor code changes on the cluster
- LLM Interface: Browser based, used via Outlook > MS365 copilot

**Workflow & Prompting strategy:**

1. **Task delegation:** The LLM handles tasks with context of the full codebase:
   - Implementation of the Scala scripts


2. **Self-Handled Tasks**:
   - Bash script for download
   - Strategy of implementation
   - Writing of report

3. **AI-assisted tasks:**
   - Task understanding
   - Scala Syntax
   - Debugging

**Key prompting steps:**

1. Read exercise instructions
2. Copy the exercise instructions into the LLM
3. Ask for any things which can be misunderstood (e.g. if for part B each of the subexercises should have their own java class implementation, or if we shall implement it all in one class)
4. Request a draft for how to structure the exercise
5. Review draft of the exercise plan and request changes where needed
6. Request Scala code
7. Review and test scala code
8. Copy any errors or warnings or weird looking outputs into the LLM for clarification and if applicable, bug fixing
9. Run code for testing and getting result.
---

## Summary of AI Tools Used

| Member                    | AI Platform            | Primary Use Case                   |
| ------------------------- | ---------------------- | ---------------------------------- |
| Eduardo Rodríguez Sánchez | Gemini 3.1 Pro Preview | Code implementation, documentation |
| Eduardo Rodríguez Sánchez | Perplexity AI          | Documentation lookup               |
| Michalina Miszkiewicz     | Microsoft Copilot      | Code Implementation                |
| Michalina Miszkiewicz     | ChatGPT                | Text correction                    |
| Henrik Klasen             | Github Copilot      | Code Implementation, Debugging     |

---

## Compliance Statement

All AI-assisted code has been reviewed for correctness and functionality. Solutions comply with the University of Luxembourg guidelines for the usage of Generative AI in teaching and learning.
