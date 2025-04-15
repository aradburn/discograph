import { discographManager } from "./DiscographManager";
import { networkManager } from "./NetworkManager";
import { relationsManager } from "./RelationsManager";

export * from "./DiscographManager";
export * from "./NetworkManager";
export * from "./RelationsManager";

// Re-export singletons from their respective files
export { discographManager, networkManager, relationsManager };

// Helper function used in svg.test.ts
export const getSelectedNodeKey = (): string | undefined => {
    const key = networkManager.selectedNodeKey;
    return typeof key === "string" ? key : undefined;
};
