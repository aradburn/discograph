import type { NodeKey, LinkKey } from "./network/data";
import type { NetworkCenter } from "./network/data";
import type { RelationsData } from "./relations";
import { getSelectedRoles } from "./roles";
import type { NodeType } from "./network/data";
import { API } from "./constants";

interface APINetworkNode {
    cluster?: number;
    distance?: number;
    id: string;
    key: NodeKey;
    links?: APINetworkLink[];
    missing?: number;
    name: string;
    size: number;
    type: NodeType | string; // Allow string for backwards compatibility with API
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
    let url = API.ENDPOINTS.NETWORK(entityType, entityId);
    const roles = getSelectedRoles() || [];
    if (roles.length) {
        url += `?${new URLSearchParams({ roles: roles.join(",") }).toString()}`;
    }
    return url;
};

const getRandomURL = (): string => {
    let url = `${API.ENDPOINTS.RANDOM}?r=${Math.floor(Math.random() * API.RANDOM_MAX)}`;
    const roles = getSelectedRoles() || [];
    if (roles.length) {
        url += `&${new URLSearchParams({ roles: roles.join(",") }).toString()}`;
    }
    return url;
};

const getRadialURL = (entityKey: NodeKey): string => {
    const [entityType, entityId] = entityKey.split("-");
    return API.ENDPOINTS.RELATIONS(entityType, entityId);
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
