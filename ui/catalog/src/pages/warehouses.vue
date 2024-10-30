<template>
  <v-container>
    <v-row>
      <v-col cols="6">
        <v-btn class="mb-6">Add Warehouse</v-btn>

        <v-treeview
          v-if="!warehouseIsEmpty"
          :items="treeItems.items"
          item-key="id"
          item-title="title"
          :load-children="loadTree"
          select-strategy="independent"
          @click:select="updateSelected"
          v-model:selected="selected"
          open-strategy="list"
          return-object
        >
          <template #prepend="{ item }">
            <v-icon v-if="item.itemType == 'project'"> mdi-bookmark</v-icon>
            <v-icon v-else-if="item.itemType == 'warehouse'">
              mdi-database
            </v-icon>
            <v-icon v-else-if="item.itemType == 'namespace'">
              mdi-folder
            </v-icon>
            <v-icon v-else-if="item.itemType == 'table'"> mdi-table</v-icon>
            <v-icon v-else-if="item.itemType == 'view'">
              mdi-view-grid-outline
            </v-icon>
            <v-icon v-else>mdi-table</v-icon>
          </template>
        </v-treeview>
        <div>Create your first Warehouse</div>
      </v-col>
      <v-col cols="6" v-if="!warehouseIsEmpty">
        <v-card>
          <v-card-title>Details {{ type }}: {{ obejctName }}</v-card-title>
          <v-card-text>
            <pre class="json-pre">{{ json }}</pre>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script lang="ts" setup>
import * as env from "../app.config";
import { ref, onMounted, reactive } from "vue";
import {
  Data,
  Namespaces,
  Tables,
  TreeItem,
  TreeItems,
  Warehouse,
} from "../common/interfaces";
import { useUserStore } from "@/stores/user";
import { useVisualStore } from "@/stores/visual";

const user = useUserStore();
const visual = useVisualStore();
const proejctId = computed(() => {
  return visual.projectSelected["project-id"];
});

const treeItems = ref<TreeItems>({ items: [] });
const access_token = user.getUser().access_token;
const warehouseIsEmpty = ref(true);
const selected = ref([]);
const json = reactive({});
const baseUrl = env.icebergCatalogUrl as string;
const managementUrl = baseUrl + "/management/v1";
const catalogUrl = baseUrl + "/catalog/v1";
const type = ref();
const obejctName = ref();
const ip = reactive({
  itemType: "projectLevel",
});

onMounted(async () => {
  try {
    await loadTree(ip);
  } catch (err) {
    console.error("Failed to load data:", err);
  }
});

async function loadData(
  subPath: string
): Promise<Data | { warehouses: Warehouse[] } | Namespaces | Tables> {
  const res = await fetch(subPath, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${access_token}`,
      "Content-Type": "application/json", // optional, if you're sending JSON data
    },
    // body: JSON.stringify(data), // optional, for POST or PUT requests
  });
  if (!res.ok) {
    throw new Error(`HTTP error! status: ${res.status}`);
  }
  return await res.json();
}

async function loadTree(item: any) {
  if (item.itemType == "projectLevel") {
    const warehousesResponse = (await loadData(
      managementUrl + "/warehouse?project-id=" + proejctId.value
    )) as { warehouses: Warehouse[] };

    if (warehousesResponse.warehouses.length != 0) {
      const children: TreeItem[] = warehousesResponse.warehouses.map(
        (warehouse) => ({
          id: warehouse.id,
          proejctId: item.id,
          itemType: "warehouse",
          title: warehouse.name,
          children: [],
        })
      );
      item.children.push(...children);
    } else {
      warehouseIsEmpty.value = true;
    }
  } else if (item.itemType == "warehouse") {
    const namespacesResponse = (await loadData(
      catalogUrl + "/" + item.id + "/namespaces"
    )) as Namespaces;

    const children: TreeItem[] = namespacesResponse.namespaces.flatMap(
      (namespaceArray) =>
        namespaceArray.map((namespace) => ({
          id: namespace,
          projectId: item.proejctId,
          whId: item.id,
          itemType: "namespace",
          title: namespace,
          children: [],
        }))
    );
    item.children.push(...children);

    return item;
  } else if (item.itemType == "namespace") {
    const tablesResponse = (await loadData(
      catalogUrl + "/" + item.whId + "/namespaces/" + item.id + "/tables"
    )) as Tables;

    const children: TreeItem[] = tablesResponse.identifiers.flatMap(
      (identifier) =>
        identifier.namespace.map((namespace) => ({
          id: `${identifier.name}`,
          projectId: item.proejctId,
          whId: item.whId,
          nsId: item.id,
          itemType: "table",
          title: `${identifier.name}`,
          children: [],
        }))
    );
    item.children.push(...children);

    const viewsResponse = (await loadData(
      catalogUrl + "/" + item.whId + "/namespaces/" + item.id + "/views"
    )) as Tables;

    const children_v: TreeItem[] = viewsResponse.identifiers.flatMap(
      (identifier) =>
        identifier.namespace.map((namespace) => ({
          id: `${identifier.name}`,
          projectId: item.proejctId,
          whId: item.whId,
          nsId: item.id,
          itemType: "view",
          title: `${identifier.name}`,
          children: [],
        }))
    );
    item.children.push(...children_v);

    return item;
  } else if (item.itemType == "table" || item.itemType == "view") {
    const res = await fetch(
      `${catalogUrl}/${item.whId}/namespaces/${item.nsId}/${item.itemType}s/${item.id}`
    );
    if (!res.ok) {
      throw new Error(`HTTP error! status: ${res.status}`);
    }

    type.value = item.itemType;
    obejctName.value = item.id;
    Object.assign(json, await res.json());
  }
}

function updateSelected(selectedItems: any) {
  console.log(selectedItems);
  console.log(treeItems.value);
  console.log(selected.value);
}
</script>

<style>
.json-pre {
  max-height: 80vh;
  overflow: auto;
}
</style>
