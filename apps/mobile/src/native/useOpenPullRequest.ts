import { useNavigation, useRoute, StackActions } from "@react-navigation/native";
import { resolveIssueLinkTarget } from "@t3tools/client-runtime/issue-links";
import { nativePullRequestDestination } from "../lib/pullRequestNavigation";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { useProjects, useThreadShell } from "../state/entities";
import { tryOpenExternalUrl } from "../lib/openExternalUrl";
import { appAtomRegistry } from "../state/atom-registry";
import { environmentServerConfigsAtom } from "../state/server";
import { composerPullRequests } from "../state/pull-requests";
import { useAtomQueryRunner } from "../state/use-atom-query-runner";
import { withKnownPullRequestSummary } from "../state/use-thread-pr";

let linkNavigationId = 0;
const nextLinkNavigationId = () => ++linkNavigationId;

type PullRequestContext = { readonly environmentId: string; readonly projectId: string };

// Avoid useRoute so layouts without route context can open any thread's pull requests.
export function useNavigateToPullRequest(options?: { readonly replace?: boolean }) {
  const navigation = useNavigation();
  const projects = useProjects();
  const replace = options?.replace;
  const readPreview = useAtomQueryRunner(composerPullRequests.preview, {
    reportFailure: false,
    reportDefect: false,
  });
  return async (url: string, context: PullRequestContext | null) => {
    // Read at press time so a config push does not hand every link a new handler.
    const serverConfigs = appAtomRegistry.get(environmentServerConfigsAtom);
    const destination = nativePullRequestDestination(url, context, projects, (environmentId) =>
      serverConfigs.has(environmentId),
    );
    if (!destination) {
      // An issue link from the same text opens the issue; anything else leaves for the browser.
      const environmentId = context && EnvironmentId.make(context.environmentId);
      const issue =
        environmentId && serverConfigs.get(environmentId)?.environment.capabilities.issues === true
          ? resolveIssueLinkTarget({
              url,
              projects: projects.filter((project) => project.environmentId === environmentId),
              preferredProjectId: context.projectId,
            })
          : null;
      if (!issue || !environmentId) return tryOpenExternalUrl(url, "pull-request");
      // A host writes `#12` as an issue link whether 12 is an issue or a pull request.
      const number = Number(issue.ref.key);
      if (Number.isInteger(number) && number > 0) {
        const pullRequest = {
          projectId: issue.ref.projectId,
          ...(issue.ref.host === undefined ? {} : { host: issue.ref.host }),
          repository: issue.ref.scope,
          number,
        };
        const preview = await readPreview({ environmentId, input: pullRequest });
        if (preview._tag === "Success") {
          const params = withKnownPullRequestSummary({ ...pullRequest, environmentId });
          if (replace) navigation.dispatch(StackActions.replace("PullRequest", params));
          else navigation.dispatch(StackActions.push("PullRequest", params));
          return true;
        }
      }
      const issueParams = {
        environmentId,
        projectId: issue.ref.projectId,
        tracker: issue.ref.tracker,
        ...(issue.ref.host === undefined ? {} : { host: issue.ref.host }),
        scope: issue.ref.scope,
        issueKey: issue.ref.key,
      };
      if (replace) navigation.dispatch(StackActions.replace("Issue", issueParams));
      else navigation.dispatch(StackActions.push("Issue", issueParams));
      return true;
    }
    const params = withKnownPullRequestSummary({
      ...destination,
      ...(destination.navigationUrl ? { navigationId: nextLinkNavigationId() } : {}),
    });
    if (replace) navigation.dispatch(StackActions.replace("PullRequest", params));
    else navigation.dispatch(StackActions.push("PullRequest", params));
    return true;
  };
}

export function useOpenPullRequest(options?: {
  readonly replace?: boolean;
  readonly context?: PullRequestContext;
}) {
  const route = useRoute();
  const routeContext = route.params as
    | {
        environmentId?: string | string[];
        projectId?: string | string[];
        threadId?: string | string[];
      }
    | undefined;
  const first = (value: string | string[] | undefined) => {
    const text = Array.isArray(value) ? value[0] : value;
    return text?.trim() ? text : undefined;
  };
  const environmentId = options?.context?.environmentId ?? first(routeContext?.environmentId);
  const threadId = first(routeContext?.threadId);
  const thread = useThreadShell(
    environmentId && threadId
      ? { environmentId: EnvironmentId.make(environmentId), threadId: ThreadId.make(threadId) }
      : null,
  );
  const projectId =
    options?.context?.projectId ?? first(routeContext?.projectId) ?? thread?.projectId;
  const navigateToPullRequest = useNavigateToPullRequest({ replace: options?.replace });
  return (url: string) =>
    navigateToPullRequest(
      url,
      environmentId && projectId
        ? { environmentId: String(environmentId), projectId: String(projectId) }
        : null,
    );
}
