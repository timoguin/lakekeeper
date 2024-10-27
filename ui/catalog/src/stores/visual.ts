// Utilities
import { defineStore } from "pinia";
import { ProjectCatalog } from "@/common/interfaces";

export const useVisualStore = defineStore(
  "visual",
  () => {
    const themeLight = ref(true);
    const navBarShow = ref(true);
    const project = reactive<ProjectCatalog>({
      version: "0.4.1",
      bootstrapped: true,
      "server-id": "00000000-0000-0000-0000-000000000000",
      "default-project-id": "00000000-0000-0000-0000-000000000000",
      "authz-backend": "allow-all",
    });

    function toggleThemeLight() {
      themeLight.value = !themeLight.value;
    }

    function navBarSwitch() {
      navBarShow.value = !navBarShow.value;
    }

    function setProjectCatalog(projectCatalog: ProjectCatalog) {
      Object.assign(project, projectCatalog);
    }

    return {
      themeLight,
      navBarShow,
      project,
      toggleThemeLight,
      navBarSwitch,
      setProjectCatalog,
    };
  },
  {
    persistedState: {
      key: "visual",
      persist: true,
    },
  }
);
