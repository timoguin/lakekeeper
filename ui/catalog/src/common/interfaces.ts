export interface Project {
  project_id: string;
  warehouses?: Warehouse[];
}

export interface Data {
  projects: Project[];
}

export interface StorageProfile {
  type: string;
  bucket: string;
  "key-prefix": string;
  "assume-role-arn": string | null;
  endpoint: string;
  region: string;
  "path-style-access": boolean;
}

export interface Warehouse {
  id: string;
  name: string;
  "project-id": string;
  "storage-profile": StorageProfile;
  status: string;
}

export interface Namespaces {
  namespaces: string[][];
}

export interface Table {
  namespace: string[];
  name: string;
}

export interface Tables {
  identifiers: Table[];
}

export interface TreeItem {
  id: string;
  projectId?: string;
  whId?: string;
  nsId?: string;
  itemType: string;
  title: string;
  children?: TreeItem[];
}

export interface TreeItems {
  items: TreeItem[];
}

export interface User {
  access_token: string;
  id_token: string;
  email: string;
  preferred_username: string;
  family_name: string;
  given_name: string;
}
