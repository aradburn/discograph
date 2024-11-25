function dg_roles_init() {
    if (dgRoles) {
        $('#jstree_div').jstree(dgRoles);
    }
}

function dg_isNumeric(obj) {
    return !Array.isArray( obj ) && (obj - parseFloat( obj ) + 1) >= 0;
}

function dg_roles_get_selected() {
    var selected_roles = $("#jstree_div").jstree().get_selected(true);
    console.log("Selected roles: ", selected_roles);

    var keysToRemove = [];
    selected_roles.forEach(role_entry => {
        console.log("role_entry: ", role_entry);
        if (! dg_isNumeric(role_entry.id)) {
            keysToRemove.push(...role_entry.children);
        }
    });
    console.log("keysToRemove: ", keysToRemove);

    var pruned_roles = [];
    selected_roles.forEach(role_entry => {
        if (! keysToRemove.includes(role_entry.id)) {
            pruned_roles.push(role_entry.text);
        }
    });

    console.log("Pruned roles: ", pruned_roles);
    return pruned_roles;
}