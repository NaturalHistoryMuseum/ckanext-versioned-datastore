ckan.module('vds_search', function () {
    let self = null;
    return {
        initialize: function () {
            // get ourselves a consistent reference to this which always points to the module
            self = this;

            self.app = new Vue({
                el: '#searchApp',
                data: {
                    message: 'hey!'
                },
                delimiters: ['[[',']]']
            });
        },
    };
});
