-- External trigger for the gexicon-blob GitHub workflow.
-- GitHub's own cron fires hours late. pg_cron fires on the minute and calls
-- the workflow's manual trigger (workflow_dispatch) over the GitHub API.
--
-- Run once in the Supabase SQL editor of an ACTIVE project (the market
-- terminal project; free-tier projects that pause would stop the cron).
-- Then store the GitHub credential (see the last block). Safe to re-run.

create extension if not exists pg_cron with schema pg_catalog;
create extension if not exists pg_net with schema extensions;
create extension if not exists supabase_vault with schema vault;

create schema if not exists ops;

create or replace function ops.gexicon_dispatch()
returns bigint
language plpgsql
security definer
set search_path = public, extensions, vault
as $$
declare
  tok text;
  rid bigint;
begin
  select decrypted_secret into tok
    from vault.decrypted_secrets
   where name = 'github_gexicon_pat'
   limit 1;
  if tok is null then
    raise notice 'gexicon_dispatch: no vault secret named github_gexicon_pat';
    return null;
  end if;
  select net.http_post(
    url     := 'https://api.github.com/repos/SBApplications/gexicon-blob/actions/workflows/blob.yml/dispatches',
    headers := jsonb_build_object(
      'Authorization', 'Bearer ' || tok,
      'Accept', 'application/vnd.github+json',
      'X-GitHub-Api-Version', '2022-11-28',
      'User-Agent', 'gexicon-dispatch',
      'Content-Type', 'application/json'),
    body    := '{"ref":"main"}'::jsonb
  ) into rid;
  return rid;
end
$$;

revoke all on function ops.gexicon_dispatch() from public;

-- Hourly at :07, Monday to Friday, matching .github/workflows/blob.yml.
-- pg_cron runs in UTC. The workflow's commit-if-changed step means an hour
-- with no new data costs nothing.
select cron.unschedule(jobname) from cron.job where jobname like 'gexicon-%';
select cron.schedule('gexicon-hourly', '7 * * * 1-5', 'select ops.gexicon_dispatch()');

-- CREDENTIAL. Create a fine-grained GitHub personal access token:
--   github.com > Settings > Developer settings > Fine-grained tokens
--   Resource owner: SBApplications. Repository: gexicon-blob only.
--   Permissions: Actions = Read and write. Nothing else. Expiry: 1 year.
-- Then run this ONE line yourself with the value pasted in (never commit it):
--
--   select vault.create_secret('<paste value here>', 'github_gexicon_pat');
--
-- Test:  select ops.gexicon_dispatch();
-- then check github.com/SBApplications/gexicon-blob/actions for a new
-- "workflow_dispatch" run within a minute.
--
-- Inspect:  select jobname, schedule, active from cron.job where jobname like 'gexicon-%';
--           select status_code, created from net._http_response order by created desc limit 5;
-- Remove:   select cron.unschedule('gexicon-hourly');
