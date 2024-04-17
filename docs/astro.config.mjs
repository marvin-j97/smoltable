import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// https://astro.build/config
export default defineConfig({
  base: "/smoltable",
  integrations: [
    starlight({
      title: "Smoltable",
      editLink: {
        baseUrl: "https://github.com/marvin-j97/smoltable/edit/main/docs",
      },
      social: {
        github: "https://github.com/marvin-j97/smoltable",
      },
      sidebar: [
        {
          label: "Getting started",
          items: [
            { label: "What is Smoltable?", link: "/" },
            { label: "Installation", link: "/guides/installation/" },
          ],
        },
        {
          label: "Guides",
          items: [
            {
              label: "Wide-column data design",
              link: "/guides/wide-column-intro/",
            },
            { label: "Column keys", link: "/guides/column-keys" },
            { label: "Locality groups", link: "/guides/locality-groups" },
            { label: "Data retention", link: "/guides/data-retention" },
          ],
        },
        {
          label: "Reference",
          autogenerate: { directory: "reference" },
          items: [
            { label: "Environment variables", link: "/reference/env" },
            {
              label: "JSON API",
              items: [
                {
                  label: "Create a table",
                  link: "/reference/json-api/create-table",
                },
                {
                  label: "Create column families",
                  link: "/reference/json-api/create-column-families",
                },
                {
                  label: "Ingest data",
                  link: "/reference/json-api/ingest-data",
                },
                {
                  label: "Retrieve rows",
                  link: "/reference/json-api/retrieve-rows",
                },
                { label: "Scan rows", link: "/reference/json-api/scan-rows" },
              ],
            },
          ],
        },
      ],
    }),
  ],
});
