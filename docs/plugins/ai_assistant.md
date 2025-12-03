
## Threat hunting AI-Assistant

This Plugin implements an extension for Gulp that delegates threat-hunting analysis and correlation of log events to an external LLM (via the OpenRouter chat completions API). It exposes a single API endpoint that accepts up to 10 log event dictionaries and streams back an AI-generated analysis to clients through an internal Redis broker.

## Prompt:
```
# Role and Goal

You are a security analyst. Your task is to analyze a set of log events provided in a JSON array. The logs come from different systems (like firewalls, web servers, and Windows endpoints).

**This is the most important instruction:**

The JSON data provides a field called `event.original`. This field contains the raw, unstructured log text. **You MUST parse this `event.original` text** to find entities like IP addresses, hostnames, usernames, file hashes, or process names.

## Your Goal: You must perform three tasks

1. **Analyze Events:** Briefly explain what each individual event means.

2. **Assess Correlation:** Determine if the events are **CORRELATED** or **NOT CORRELATED**.

3. **Explain Findings:**

- If **CORRELATED**, explain *how* they are linked (which entities do they share?) and describe the sequence of the incident.

- If **NOT CORRELATED**, state this clearly.

## Correlation Rules

Events are **CORRELATED** if they meet both conditions:

1. They share at least one common entity (indicator) that you **find inside the `event.original` text** (such as a shared IP address, hostname, username, file hash, or process name). Entities must be exact matches (use case-insensitive).

Assume events are provided in the order they appear in the JSON array, but sort them chronologically by the `timestamp` field when describing sequences. If events lack required fields (e.g., `agent.type`), note this in your analysis and proceed with available data. For multiple events, correlation applies to the entire set: all events must share at least one entity with at least one other event in the set, forming a connected chain.

## Input Format Example

The input is a JSON array. **This is a description about any fields inside JSON object.

```json

[
{
	"@timestamp": "The ISO 8601 timestamp for when the event occurred.",
	"gulp.source_id": "The unique ID for the log source (e.g., a specific log file).",
	"gulp.context_id": "The unique ID for the context (e.g., a hostname or server).",
	"agent.type": "The tool or plugin that generated the log (e.g., 'sysmon', 'firewall').",
	"event.original": "The orginal log write by agent.type"
}
]

Input Data:

<%DATA%>
  

## Required Output Format

You must provide your analysis in Markdown using this exact structure:
1. **Event Analysis**
- Event 1 (agent.type: [agent_type]): [brief event description]
- Event 2 (agent.type: [agent_type]): [brief event description]
- ... (Continue for all events)
1. **Correlation Assessment**
(State here: CORRELATED or NOT CORRELATED)
2. **Incident Summary**
(If NOT CORRELATED, write: "The events do not share common indicators and are considered unrelated.")
(If CORRELATED, explain the incident sequence here.)

```


### Configuration:

Requires the following configuration in the GULP configuration file:

```json
"ai_report": {
	// The default model to use.
	"default_model": "your_default_model_here",
	// Insert your OpenRouter API key here.
	"openrouter_key": "sk-or-your-openrouter-api-key",
}
```

### Exposed API:

The plugin exposes a single HTTP endpoint:  

- Endpoint: `/get_ai_hint`
	- Method: `POST`
#### Input Data Requirements

To use the API, the client must provide the following parameters via query string:

- token (send in header request),
- operation_id (mandatory, used to check user permission),
- ws_id (mandatory, used to send the response via stream),
- req_if (optional, if not set, gulp generate a unique req_id and us it inside response)  

and send data via body:

- data payload must be a list of JSON objects (maximum 10 items) representing the logs to be analyzed.

Each object in the list requires the following fields:

- event.original: The raw, unstructured text of the log (e.g., the full log line from IIS or a firewall).
- timestamp: The time the event occurred, used by the AI to establish a timeline.
- agent.type: The source or tool that generated the log (e.g., "iis", "windows-eventlog", "firewall").
- gulp.source_id: The unique ID for the log source (e.g., a specific log file).
- gulp.context_id: "The unique ID for the context (e.g., a hostname or server).",

### Response:

The API operates asynchronously. Upon receiving a request, it performs permission checks and immediately returns a JSON response with a `pending` status.
	- Events written to the Redis broker (GulpRedisBroker) include: 
		- "ai_assistant_stream": partial streamed content ("payload" field contains text).
		- "ai_assistant_done": indicates the stream is finished.
		- "ai_assistant_error": indicates an API error (payload" field contains error text).
	- Clients are expected to subscribe to these events to reconstruct the final AI-generated response in real time.


