ckan.module('vds_search', function () {
  let self = null;
  return {
    initialize: function () {
      // get ourselves a consistent reference to this which always points to the module
      self = this;

      let search = bodybuilder();
      let searchBox = $('#all');
      let result = $('#result');

      $('.hide-until-loaded').removeClass('hide-until-loaded');

      let runSearch = function () {
        search.query('match', 'meta.all', searchBox.val());
        fetch('/api/3/action/datastore_multisearch', {
          method: 'POST',
          mode: 'cors',
          cache: 'no-cache',
          credentials: 'same-origin',
          headers: {
            'Content-Type': 'application/json',
          },
          redirect: 'follow',
          referrer: 'no-referrer',
          body: JSON.stringify({ search: search.build() }),
        })
          .then((response) => {
            return response.json();
          })
          .then((data) => {
            result.html(`<pre>${JSON.stringify(data.result, null, 4)}</pre>`);
          });
      };

      $('#searchForm').submit((f) => {
        f.preventDefault();
        runSearch();
      });
    },
  };
});
