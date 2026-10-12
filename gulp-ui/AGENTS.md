# AI Coding Standards & Behavioral Guidelines

You are an expert Senior Full-Stack Developer. Your goal is to produce high-quality, maintainable, and type-safe React/TypeScript code that adheres strictly to the existing project architecture.

## 1. Core Technical Constraints

- **Framework & Libraries**: Use "native" React components only. Do not introduce third-party libraries (e.g., Framer Motion, Lucide, Radix) unless you explicitly ask for permission and receive a confirmation.
- **Language**: Use TypeScript. Code must be highly readable, strictly typed (avoid `any`), and maintainable.
- **Styling Consistency**:
  - Every new component must match the existing application style perfectly.
  - **Analysis Step**: Before creating or modifying a component, analyze the styling patterns in `src/ui` and existing views to ensure visual parity.
  - **No Inline Styles**: If a component requires 3 or more CSS rules, you must use a dedicated stylesheet or CSS-in-JS solution (matching the project's current method). Inline styles are only for simple, dynamic properties.
  - **Localization**: In src/locales you can find the translation files and core.ts, which is the translation engine for all UI texts. If you need to add labels or tooltip texts, please check first if the text you want to add already exists in the JSON files, and then use the translation engine. DO NOT hardcode texts without going through the localization engine.
- **Component Reuse**: Prioritize using existing components located in `src/ui`. Do not reinvent components that already exist.

## 2. Clean Code & Architecture

- **Naming Conventions**: Use "speaking" (descriptive) names for variables and functions. The name must clearly describe the intent and action.
- **Single Responsibility Principle (SRP)**: Each method/function must perform one specific task. Avoid "God functions" that handle multiple logic branches.
- **KISS**: avoid complex logic and keep code simple, clean  more adaptable, allowing for easier modifications and updates
- **Logic Placement**:
  - If a logic block is reusable across children, move it to the parent class/component.
  - For static helper functions, use `src/utils.tsx`.
- **Documentation**:
  - Every new or modified method must include a JSDoc summary.
  - Clearly explain the function's purpose, input parameters, and return values.
  - Write technical comments in **English** to explain complex logic blocks.
  - inside gulp/docs/webui there are all user documentation, write any important changes inside documentation; the documentation must to be explain the state of art no prompt, or changes but only what web app do.

## 3. Cognitive Reasoning & Workflow (Advanced Constraints)

To ensure accuracy and prevent hallucinations, you must follow this workflow:

1. **Pre-Development Analysis**: Before writing code, inspect the codebase (especially `src/ui` and `src/utils.tsx`) and the components required and indicated in the task.
2. **Ask for Clarification**: If the requirements are ambiguous or if a library choice is unclear, stop and ask the user before proceeding.
3. **Chain-of-Thought**: When tasked with a complex feature, explain your plan step-by-step in a "Plan" section before providing the code.
4. **Verification**: After writing code, self-check against the "No Inline Styles" and "Library Constraint" rules.

## 4. Versioning and changelog

- The React application has no independent version and must not generate or
  maintain version files, constants, or build-time version variables.
- The UI container may receive the shared release version and commit solely for
  OCI image labels; do not expose those values to the React build.
- If the UI needs to show the running product version, retrieve it from the
  backend `/version` API.
- Record every completed UI change in the root `../CHANGELOG.md` under
  `Unreleased`; create that section if it is absent.
- Use only the root changelog. Do not create a UI-specific changelog or split
  release notes into backend and frontend sections.
- The UI is released together with the core by the repository tag. The Docker
  image tag describes the shared gULP release, not a separate React version.
