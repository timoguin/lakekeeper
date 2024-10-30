<template>
  <v-app>
    <v-main>
      <router-view />
    </v-main>
  </v-app>
</template>

<script lang="ts" setup>
import { onMounted } from "vue";
import { idpOn } from "@/app.config";
import router from "@/router";
import { useVisualStore } from "@/stores/visual";
import { useFunctions } from "@/plugins/functions";

// import { useUserStore } from "@/stores/user";
// import { ProjectCatalog } from "@/common/interfaces";
// import { icebergCatalogUrl } from "@/app.config";

const visual = useVisualStore();
const functions = useFunctions();
// const user = computed(() => {
//   return useUserStore().getUser();
// });

// const isAuthenticated = computed(() => {
//   return useUserStore().isAuthenticated;
// });
// const auth = useAuth();

onMounted(async () => {
  if (!idpOn) {
    await functions.getServerInfo();

    if (!visual.project.bootstrapped) router.push("/bootstrap");
  }
});

// async function getServerInfo() {
//   try {
//     const response = await fetch(`${icebergCatalogUrl}/management/v1/info`, {
//       method: "GET", // HTTP method
//       headers: {
//         "Content-Type": "application/json", // Specify the content type
//       },
//     });
//     if (!response.ok) {
//       throw new Error(`Error: ${response.statusText}`);
//     }
//     const data: ProjectCatalog = await response.json();
//     visual.setProjectCatalog(data);
//   } catch (error) {}
// }
</script>
