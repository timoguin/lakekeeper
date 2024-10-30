import { App } from "vue";
import { useUserStore } from "@/stores/user";
import { useVisualStore } from "@/stores/visual";
import * as env from "@/app.config";
import { ProjectCatalog } from "@/common/interfaces";

const baseUrl = env.icebergCatalogUrl as string;
const managementUrl = baseUrl + "/management/v1";

async function getServerInfo() {
  try {
    const visualStore = useVisualStore();
    const response = await fetch(`${managementUrl}/info`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    });
    if (!response.ok) {
      throw new Error(`Error: ${response.statusText}`);
    }
    const data: ProjectCatalog = await response.json();
    visualStore.setProjectCatalog(data);
  } catch (error) {}
}

const loadProjectList = async () => {
  try {
    const visualStore = useVisualStore();
    const userStore = useUserStore();
    const res = await fetch(managementUrl + "/project-list", {
      method: "GET",
      headers: {
        Authorization: `Bearer ${userStore.user.access_token}`,
        "Content-Type": "application/json",
      },
    });
    if (!res.ok) {
      throw new Error(`HTTP error! status: ${res.status}`);
    }
    const projects = await res.json();
    visualStore.setProjectList(projects.projects);

    for (const proj of projects.projects) {
      Object.assign(visualStore.projectSelected, proj);
    }
  } catch (error) {
    console.error("Failed to load projects", error);
    return error;
  }
};

export function useFunctions() {
  return {
    getServerInfo,
    loadProjectList,
  };
}

export default {
  install: (app: App) => {
    const functions = useFunctions();
    app.provide("functions", functions);
    app.config.globalProperties.$functions = functions;
  },
};
