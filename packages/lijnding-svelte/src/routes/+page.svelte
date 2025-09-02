<script lang="ts">
  import { onMount } from 'svelte';

  type Run = {
    run_id: string;
  };

  let runs: Run[] = [];
  let error: string | null = null;
  const API_BASE_URL = 'http://localhost:8000';

  onMount(async () => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/runs`);
      if (!response.ok) {
        throw new Error('Failed to fetch runs');
      }
      runs = await response.json();
    } catch (e: any) {
      error = e.message;
    }
  });
</script>

<svelte:head>
  <title>LijnDing Pipeline Runs</title>
</svelte:head>

<div class="container mx-auto p-4">
  <h1 class="text-2xl font-bold mb-4">Pipeline Runs</h1>

  {#if error}
    <div class="text-red-500 bg-red-100 p-4 rounded">
      <strong>Error:</strong> {error}
    </div>
  {:else if runs.length === 0}
    <p>No pipeline runs found. Run a pipeline with the `--gui` flag to see it here.</p>
  {:else}
    <ul class="space-y-2">
      {#each runs as run}
        <li class="p-4 bg-white shadow rounded hover:bg-gray-50">
          <a href="/run/{run.run_id}" class="block">
            <span class="font-mono">{run.run_id}</span>
          </a>
        </li>
      {/each}
    </ul>
  {/if}
</div>
