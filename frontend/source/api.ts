import type { NodeKey, LinkKey } from "./network/data";
import type { NetworkCenter } from "./network/data";
import type { RelationsData } from "./relations";
import { getSelectedRoles } from "./roles";

interface APINetworkNode {
    cluster?: number;
    distance?: number;
    id: string;
    key: NodeKey;
    links?: APINetworkLink[];
    missing?: number;
    name: string;
    size: number;
    type: "label" | "artist";
}

interface APINetworkLink {
    key: LinkKey;
    role: string;
    source: NodeKey;
    target: NodeKey;
}

export interface APINetworkDataResponse {
    center: {
        key: NodeKey;
        name: string;
    };
    nodes: APINetworkNode[];
    links: APINetworkLink[];
}

const getNetworkURL = (entityKey: NodeKey): string => {
    const [entityType, entityId] = entityKey.split("-");
    let url = `/api/${entityType}/network/${entityId}`;
    const roles = getSelectedRoles() || [];
    if (roles.length) {
        url += `?${new URLSearchParams({ roles: roles.join(",") }).toString()}`;
    }
    return url;
};

const getRandomURL = (): string => {
    let url = `/api/random?r=${Math.floor(Math.random() * 1000000)}`;
    const roles = getSelectedRoles() || [];
    if (roles.length) {
        url += `&${new URLSearchParams({ roles: roles.join(",") }).toString()}`;
    }
    return url;
};

const getRadialURL = (entityKey: NodeKey): string => {
    const [entityType, entityId] = entityKey.split("-");
    return `/api/${entityType}/relations/${entityId}`;
};

export const fetchAPINetwork = async (
    entityKey: NodeKey,
): Promise<APINetworkDataResponse> => {
    const url = getNetworkURL(entityKey);

    const response = await fetch(url);
    if (!response.ok) throw new Error(response.statusText);
    return (await response.json()) as APINetworkDataResponse;
};

export const fetchAPIRandom = async (): Promise<NetworkCenter> => {
    const url = getRandomURL();

    const response = await fetch(url);
    if (!response.ok) throw new Error(response.statusText);
    return (await response.json()) as NetworkCenter;
};

export const fetchAPIRadial = async (
    entityKey: NodeKey,
): Promise<RelationsData> => {
    const url = getRadialURL(entityKey);

    const response = await fetch(url);
    if (!response.ok) throw new Error(response.statusText);
    return (await response.json()) as RelationsData;
};
