ckan.module('vds_search', function () {
    let self = null;
    return {
        initialize: function () {
            // get ourselves a consistent reference to this which always points to the module
            self = this;

            self.app = new Vue({
                el: '#searchApp',
                delimiters: ['[[', ']]'],
                data: {
                    search: bodybuilder(),
                    result: {},
                    records: [],
                    headers: [],
                    all: "annularia",
                },
                mounted: function () {
                    // it'd be nice to use v-cloak for this but because we generally load the vue
                    // lib after the page is rendered it's too late for the elements with the attr
                    // to actually be hidden from view. This does essentially the same thing, by
                    // manually removing the hide-until-loaded class on all elements that have it
                    const hiddenElements = document.getElementsByClassName('hide-until-loaded');
                    for (let i = 0, l = hiddenElements.length; i < l; i++) {
                        hiddenElements[i].classList.remove('hide-until-loaded');
                    }
                    document.getElementById('all').focus();
                },
                methods: {
                    addAllMatch: function (event) {
                        this.search.query('match', 'meta.all', this.all);
                        this.runSearch();
                    },
                    runSearch: function () {
                        const vue = this;
                        fetch('/api/3/action/datastore_multisearch', {
                            method: 'POST',
                            mode: 'cors',
                            cache: 'no-cache',
                            credentials: 'same-origin',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            redirect: 'follow',
                            referrer: 'no-referrer',
                            body: JSON.stringify({'search': this.search.build()}),
                        }).then(response => {
                            return response.json();
                        }).then(data => {
                            vue.result = data;
                            vue.records = data.result.records;
                            const newHeaders = new Set();
                            vue.records.forEach(record => {
                                Object.keys(record.data).forEach(header => {
                                    newHeaders.add(header);
                                })
                            });
                            vue.headers = Array.from(newHeaders);
                        });
                    },
                },
                watch: {
                    result: function(result) {
                        console.log(result.length);
                    }
                }
            });
        },
    };
});
