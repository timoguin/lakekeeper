/**
 * router/index.ts
 *
 * Automatic routes for `./src/pages/*.vue`
 */

// Composables
import { createRouter, createWebHistory } from "vue-router/auto";
import { setupLayouts } from "virtual:generated-layouts";
import { routes } from "vue-router/auto-routes";
import { useUserStore } from "../stores/user";
import { useVisualStore } from "../stores/visual";
import * as env from "../app.config";

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: setupLayouts(routes),
});

// Workaround for https://github.com/vitejs/vite/issues/11804
router.onError((err, to) => {
  if (err?.message?.includes?.("Failed to fetch dynamically imported module")) {
    if (!localStorage.getItem("vuetify:dynamic-reload")) {
      console.log("Reloading page to fix dynamic import error");
      localStorage.setItem("vuetify:dynamic-reload", "true");
      location.assign(to.fullPath);
    } else {
      console.error("Dynamic import error, reloading page did not fix it", err);
    }
  } else {
    console.error(err);
  }
});

router.isReady().then(() => {
  localStorage.removeItem("vuetify:dynamic-reload");
});

router.beforeEach((to, from, next) => {
  const userStorage = useUserStore();
  const visual = useVisualStore();

  if (env.idpOn) {
    // Check if the user is authenticated and project bootstrap is not done
    if (
      userStorage.isAuthenticated &&
      !visual.projectInfo.bootstrapped &&
      to.path !== "/bootstrap"
    ) {
      return next("/bootstrap");
    }

    // Allow access to login and callback paths
    if (to.path === "/login" || to.path === "/callback") {
      return next();
    }

    // Redirect unauthenticated users to login
    if (!userStorage.isAuthenticated && to.path !== "/login") {
      return next("/login");
    }

    // Allow access if authenticated and not redirected
    next();
  } else {
    // For cases where idpOn is false

    if (!visual.projectInfo.bootstrapped && to.path !== "/bootstrap") {
      return next("/bootstrap");
    }

    if (
      to.path === "/login" ||
      to.path === "/callback" ||
      to.path === "/logout"
    ) {
      return next("/");
    }

    // Allow access to other paths
    next();
  }
});

export default router;
