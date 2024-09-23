export interface RepositoryResult {
  // Repo metadata
  repositoryName: string;
  repoNameWithOwner: string;
  licenseName: string;
  topics: string[];

  // Counts of various things
  projectsCount: number;
  projectsV2Count: number;
  discussionsCount: number;
  forksCount: number;
  totalIssuesCount: number;
  openIssuesCount: number;
  closedIssuesCount: number;
  totalPullRequestsCount: number;
  openPullRequestsCount: number;
  closedPullRequestsCount: number;
  mergedPullRequestsCount: number;
  watchersCount: number;
  starsCount: number;
  collaboratorsCount: number;
  downloadCount: number;
  totalDownloadCount: number;
  monthlyDownloadCount: number;
  weeklyDownloadCount: number;
  dailyDownloadCount: number;
  contributorsCount: number;
  condaTotalDownloads: number;
  condaMonthlyDownloads: number;

  // Flags
  discussionsEnabled: boolean;
  projectsEnabled: boolean;
  issuesEnabled: boolean;

  // Calculated metrics
  openIssuesAverageAge: number;
  openIssuesMedianAge: number;
  closedIssuesAverageAge: number;
  closedIssuesMedianAge: number;
  issuesResponseAverageAge: number;
  issuesResponseMedianAge: number;
}
