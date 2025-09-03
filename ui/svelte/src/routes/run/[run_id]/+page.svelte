<script lang="ts">
  import { page } from '$app/stores';

  type Event = {
    event: string;
    stage_name?: string;
    item?: any;
    result?: any;
    error?: string;
    time_taken?: number;
    [key: string]: any;
  };

  let events: Event[] = [];
  let error: string | null = null;
  const API_BASE_URL = 'http://localhost:8000';
  const runId = $page.params.run_id;

  async function fetchEvents() {
    try {
      const response = await fetch(`${API_BASE_URL}/api/runs/${runId}`);
      if (!response.ok) {
        throw new Error('Failed to fetch run details');
      }
      const data = await response.json();
      events = data.events;
    } catch (e: any) {
      error = e.message;
    }
  }

  fetchEvents();
</script>

<svelte:head>
  <title>Run {runId}</title>
</svelte:head>

<div class="container mx-auto p-4">
  <a href="/" class="text-blue-500 hover:underline">&larr; Back to all runs</a>
  <h1 class="text-2xl font-bold my-4">
    Run Details: <span class="font-mono">{runId}</span>
  </h1>

  {#if error}
    <div class="text-red-500 bg-red-100 p-4 rounded">
      <strong>Error:</strong> {error}
    </div>
  {:else}
    <div class="space-y-4">
      {#each events as event, i}
        <div class="p-4 bg-white shadow rounded font-mono text-sm">
          <p><strong>Event:</strong> {event.event}</p>
          {#if event.stage_name}
            <p><strong>Stage:</strong> {event.stage_name}</p>
          {/if}
          {#if event.time_taken}
            <p><strong>Time:</strong> {event.time_taken.toFixed(4)}s</p>
          {/if}
          {#if event.error}
            <p class="text-red-500"><strong>Error:</strong> {event.error}</p>
          {/if}
          <details class="mt-2">
            <summary>Raw Data</summary>
            <pre class="bg-gray-100 p-2 rounded mt-1 overflow-auto">{JSON.stringify(event, null, 2)}</pre>
          </details>
        </div>
      {/each}
    </div>
  {/if}
</div>
