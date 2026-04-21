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

**AI Platform used:** Microsoft Copilot (via University in Outlook browser)

**Development Environment:**

- Editor: Visual Studio Code for major changes (Writing the Java classes), vim/nano for minor code changes on the cluster
- LLM Interface: Browser based, used via Outlook > MS365 copilot

**Workflow & Prompting strategy:**

1. **Task delegation:** The LLM handles tasks with context of the full codebase:
   - Java class generation for the exercises part a and b
   - Proposals of the map reducer architecture for part b
   - Debugging process (primarily for imports and CLI argument fine tuning for part C, as it was not very clear how to pass multiple directories as input)

2. **Self-Handled Tasks**:
   - Bash/SBATCH script for easy execution of part C (due to LLMs lack of information on SLURM and bash environment on the cluster)
   - Writing of the README file
   - Compilation and execution of parts A and B
   - Choosing the base class for part B (based on HadoopWordPairs)

3. **AI-assisted tasks:**
   - Task understanding. First tried on my own, then pasted the task into a prompt and asked about any further ambiguities to make sure that it was correct

**Key prompting steps:**

1. Read exercise instructions
2. Copy the exercise instructions into the LLM
3. Ask for any things which can be misunderstood (e.g. if for part B each of the subexercises should have their own java class implementation, or if we shall implement it all in one class)
4. Request a draft for how to structure the exercise
5. Review draft of the exercise plan and request changes where needed
6. Request Java code (include the base class in the prompt and outline the required changes)
7. Review java class output by LLM, compile test it
8. Request any further imports missed (e.g. util functions and so on)
9. Apply fixes in java class
10. Execute the program, interpret output
    (11. applies to Part C: request instructions on how to use multiple input directories)

---

## Summary of AI Tools Used

| Member                    | AI Platform            | Primary Use Case                   |
| ------------------------- | ---------------------- | ---------------------------------- |
| Eduardo Rodríguez Sánchez | Gemini 3.1 Pro Preview | Code implementation, documentation |
| Eduardo Rodríguez Sánchez | Perplexity AI          | Documentation lookup               |
| Michalina Miszkiewicz     | Microsoft Copilot      | Code Implementation                |
| Michalina Miszkiewicz     | ChatGPT                | Text correction                    |
| Henrik Klasen             | Microsoft Copilot      | Code Implementation, Debugging     |

---

## Compliance Statement

All AI-assisted code has been reviewed for correctness and functionality. Solutions comply with the University of Luxembourg guidelines for the usage of Generative AI in teaching and learning.
