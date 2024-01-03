function dg_network_selectPage(page) {
    if ((page >= 1) && (page <= dg.network.data.pageCount)) {
        dg.network.pageData.currentPage = page;
    } else {
        dg.network.pageData.currentPage = 1;
    }
    var currentPage = dg.network.pageData.currentPage;
    var pageCount = dg.network.data.pageCount;
    if (currentPage == 1) {
        var prevPage = pageCount;
    } else {
        var prevPage = currentPage - 1;
    }
    var prevText = prevPage + ' / ' + pageCount;
    if (currentPage == pageCount) {
        var nextPage = 1;
    } else {
        var nextPage = currentPage + 1;
    }
    var nextText = nextPage + ' / ' + pageCount;
    $('#paging .previous-text').text(prevText);
    $('#paging .next-text').text(nextText);

    var filteredNodes = Array.from(dg.network.data.nodeMap.values()).filter(function(d) {
        return (d.pages.indexOf(currentPage) != -1);
    });
    var filteredLinks = Array.from(dg.network.data.linkMap.values()).filter(function(d) {
        return (d.pages.indexOf(currentPage) != -1);
    });
    dg.network.pageData.nodes.length = 0;
    dg.network.pageData.links.length = 0;
    Array.prototype.push.apply(dg.network.pageData.nodes, filteredNodes);
    Array.prototype.push.apply(dg.network.pageData.links, filteredLinks);
}