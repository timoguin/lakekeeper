// Utilities
import { defineStore } from "pinia";
import { ProjectCatalog, Project } from "@/common/interfaces";

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

    const projectSelected = reactive<Project>({
      "project-id": "0",
      "project-name": "none",
    });

    const projectList = reactive<Project[]>([]);

    function toggleThemeLight() {
      themeLight.value = !themeLight.value;
    }

    function navBarSwitch() {
      navBarShow.value = !navBarShow.value;
    }

    function setProjectCatalog(projectCatalog: ProjectCatalog) {
      Object.assign(project, projectCatalog);
    }
    function setProject(p: Project) {
      Object.assign(project, p);
    }

    function setProjectList(p: Project[]) {
      Object.assign(projectList, p);
    }

    return {
      themeLight,
      navBarShow,
      project,
      projectList,
      projectSelected,
      toggleThemeLight,
      setProjectList,
      navBarSwitch,
      setProjectCatalog,
      setProject,
    };
  },
  {
    persistedState: {
      key: "visual",
      persist: true,
    },
  }
);
