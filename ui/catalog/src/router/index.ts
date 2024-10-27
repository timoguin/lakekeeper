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
  console.log("env.idpOn", env.idpOn, typeof env.idpOn);
  if (env.idpOn) {
    console.log("env.idpOn1", env.idpOn);

    if (to.path === "/login" || to.path === "/callback") {
      return next();
    } else {
      if (!userStorage.isAuthenticated) {
        next("/login");
      } else {
        // Proceed to the route
        next();
      }
    }
  } else {
    next();
  }

  // Check if the route requires authentication
});

export default router;
