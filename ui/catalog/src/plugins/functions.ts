import { App } from "vue";
import { useUserStore } from "@/stores/user";
import { useVisualStore } from "@/stores/visual";
import * as env from "@/app.config";

const userStore = useUserStore();
const visualStore = useVisualStore();
const access_token = userStore.user.access_token;
const isAuthenticated = userStore.isAuthenticated;
const baseUrl = env.icebergCatalogUrl as string;
const managementUrl = baseUrl + "/management/v1";

const loadProjectList = async () => {
  try {
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
    access_token,
    isAuthenticated,
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
