import { CustomOctokit } from '../lib/octokit';
import { Config } from '../index';
import { Organization, Repository } from '@octokit/graphql-schema';

export const queryRepoNames = async (octokit: CustomOctokit, config: Config) => {
  const organization = await octokit.graphql.paginate<{
    organization: Organization;
  }>(
    `
  query ($cursor: String, $organization: String!) {
    organization(login:$organization) {
      repositories(privacy:PUBLIC, first:100, isFork:false, isArchived:false, after: $cursor)
      {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          name
        }
      }
    }
  }
  `,
    {
      organization: config.organization,
    },
  );

  return organization.organization.repositories.nodes!.filter(
    (repo) =>
      !(repo?.isArchived && !config.includeArchived) ||
      !(repo.isFork && !config.includeForks),
  ) as Repository[];
};