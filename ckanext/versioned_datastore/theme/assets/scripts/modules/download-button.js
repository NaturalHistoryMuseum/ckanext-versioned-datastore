ckan.module('versioned_datastore_download-button', function ($) {
  return {
    initialize: function () {
      // use the same 'this' object in all _on*() and _set*() functions in this module
      $.proxyAll(this, /_on/);
      $.proxyAll(this, /_set/);

      // get the icon and its classes so we can turn it into a spinner while loading
      this.icon = this.$('#vds-download-button-icon');
      this.iconClass = this.icon[0].className;

      // process options into searchOptions and templateOptions
      this.templateOptions = {
        multiResource: true,
        datastore: true,
      };
      this.searchOptions = {};

      if (this.options.slug_or_doi) {
        // if a slug or doi is set, ignore everything else
        this.options.resources = null;
        this.options.query = null;
        this.options.non_datastore = false;

        this.searchOptions.slug_or_doi = this.options.slug_or_doi;
        this.templateOptions.slug = this.options.slug_or_doi;
        this._setSearchUrl();
      } else {
        // resources defaults to an empty array
        this.options.resources =
          this.options.resources === undefined
            ? []
            : this.options.resources.split(',');
        this.searchOptions.resource_ids = this.options.resources;
        this.templateOptions.multiResource = this.options.resources.length > 1;

        if (this.options.non_datastore) {
          this.options.query = {};
          this.templateOptions.datastore = false;
        }

        // query can either be an object or 'FROM URL'
        if (
          typeof this.options.query === 'string' &&
          this.options.query.toUpperCase() === 'FROM URL'
        ) {
          // make sure it's consistent for other functions
          this.options.query = 'FROM URL';
          this.searchOptions.query = {}; // it'll be set when the popup is shown
        } else if (
          this.options.query === undefined ||
          !(typeof this.options.query === 'object')
        ) {
          // defaults to an empty object
          this.options.query = {};
          this.searchOptions.query = this.options.query;
        } else {
          this.searchOptions.query = this.options.query;
        }
      }

      // set up event handlers
      this.el.on('click', this._onClick);
      this.el.on('shown.bs.popover', this._onShowPopover);
      this.sandbox.subscribe('vds_dl_popover_shown', this._onEvent);
    },

    teardown: function () {
      this.sandbox.unsubscribe('vds_dl_popover_shown', this._onEvent);
    },

    _snippetReceived: false,

    _onReceiveSnippet: function (html) {
      this.el.popover({
        html: true,
        sanitize: false,
        content: html,
        placement: 'bottom',
        viewport: {
          selector: 'body',
          padding: 20,
        },
        trigger: 'manual', // it has to be manual so we can hide/cancel from inside it
      });
      this.el.popover('show');
      this._setLoading(false);
    },

    _onReceiveSnippetError: function (error) {
      this._flashError();
    },

    _onClick: function (event) {
      if (!this._snippetReceived) {
        this._setLoading(true);
        this.sandbox.client.getTemplate(
          'download_popup.html',
          this.templateOptions,
          this._onReceiveSnippet,
          this._onReceiveSnippetError,
        );
        this._snippetReceived = true;
      } else {
        this.el.popover('toggle');
      }
    },

    _onShowPopover: function (event) {
      // emit/publish event for other download buttons to listen to
      this.sandbox.publish('vds_dl_popover_shown', this.el);

      // every time we show the popover it creates a new instance with a new id, so we
      // have to set up all the listeners again

      this.popoverId = this.el.attr('aria-describedby');
      this.popover = $(`#${this.popoverId}`);
      this.popoverForm = this.popover.find('form');

      // hide it when the cancel button is clicked
      this.popoverForm.on('reset', () => {
        this.el.popover('hide');
      });

      // hide/show the email group when user changes notif type
      const notifierSelect = this.popoverForm.find('#vds-dl-notifier');
      notifierSelect.on('change', () => {
        let emailGroup = this.popoverForm.find('#vds-dl-email-group');
        let emailBox = this.popoverForm.find('#vds-dl-email');
        if (notifierSelect.val() === 'email') {
          emailGroup.removeClass('hidden');
          emailBox.attr('required', true);
        } else {
          emailGroup.addClass('hidden');
          emailBox.removeAttr('required');
        }
      });

      // prevent the form submitting until we're ready
      const submitButton = this.popoverForm.find('#vds-dl-submit');
      submitButton.attr('disabled', true);
      this.popoverForm.on('submit', (e) => {
        e.preventDefault();
      });

      // set the query and make sure the search page link is correct
      this._setQuery().then(() => {
        this.popover
          .find('#vds-dl-search-link')
          .attr('href', this.templateOptions.searchUrl);
        this.popoverForm.on('submit', this._onSubmit);
        submitButton.removeAttr('disabled');
      });
    },

    _onSubmit: function (event) {
      event.preventDefault();
      let formData = { query: { ...this.searchOptions } };
      this.popoverForm
        .serializeArray()
        .filter((i) => {
          let element = this.popoverForm.find(`[name="${i.name}"]`);
          return element.is(':visible');
        })
        .forEach((i) => {
          let nameParts = i.name.split('.');
          nameParts.reduce((parentContainer, part, ix) => {
            if (!Object.keys(parentContainer).includes(part)) {
              let val;
              switch (i.value) {
                case 'on':
                  val = true;
                  break;
                case 'off':
                  val = false;
                  break;
                default:
                  val = i.value;
                  break;
              }
              // this retains a reference to formData, so we're just setting nested
              // properties on that
              parentContainer[part] = ix === nameParts.length - 1 ? val : {};
            }
            return parentContainer[part];
          }, formData);
        });

      if (this.options.non_datastore) {
        formData.file = {
          format: 'raw',
          format_args: { allow_non_datastore: true },
        };
      }

      this._setLoading(true);
      this.sandbox.client.call(
        'POST',
        'datastore_queue_download',
        formData,
        (response) => {
          if (response.success) {
            this.popoverForm.addClass('hidden');
            this.popover
              .find('#vds-dl-status-link')
              .attr(
                'href',
                this.sandbox.client.endpoint +
                  '/status/download/' +
                  response.result.download_id,
              );
            this.popover.find('#vds-dl-post-submit').removeClass('hidden');
          } else {
            this._flashError();
          }
          this._setLoading(false);
        },
        (error) => {
          this._flashError();
          this._setLoading(false);
          console.error(error);
        },
      );
    },

    _onEvent: function (button) {
      if (button !== this.el) {
        this.el.popover('hide');
      }
    },

    _setLoading: function (isLoading) {
      this.icon.removeClass();
      this.icon.addClass(isLoading ? 'fas fa-spinner fa-spin' : this.iconClass);
    },

    _flashError: function (message) {
      message =
        message ||
        'Unable to download at the moment. Try again later, contact ' +
          'us, or try downloading from the search page.';
      let errorMessages = $('.flash-messages .vds-dl-error');
      if (errorMessages.length === 0) {
        $('.flash-messages').append(
          '<div class="alert alert-error vds-dl-error">' + message + '</div>',
        );
      }
    },

    _removeErrors: function () {
      $('.flash-messages .vds-dl-error').remove();
    },

    _setSearchUrl: function () {
      this.templateOptions['searchUrl'] =
        this.sandbox.client.endpoint +
        '/search/' +
        (this.templateOptions.slug || '');
    },

    _setQuery: function () {
      if (this.options.query === 'FROM URL') {
        let params = new URLSearchParams(window.location.search);
        this.searchOptions.query = {};
        if (params.has('q') || params.has('filters')) {
          if (params.has('q') && params.get('q') !== '') {
            this.searchOptions.query['q'] = params.get('q');
          }
          if (params.has('filters') && params.get('filters') !== '') {
            let filters = {};
            params
              .get('filters')
              .split('|')
              .forEach((f) => {
                let filterParts = f.split(':');
                filters[filterParts[0]] = filterParts.slice(1).join(':');
              });
            this.searchOptions.query['filters'] = filters;
          }
          this.searchOptions.query_version = 'v0'; // show it needs converting
        }
        return this._setSlug();
      } else if (!this.templateOptions.slug && !this.options.non_datastore) {
        return this._setSlug();
      } else {
        return new Promise((resolve, reject) => {
          resolve();
        });
      }
    },

    _setSlug: function () {
      // get a slug for this search; it doesn't really matter if this fails, but it's nice
      this._setLoading(true);
      let slugOptions = {
        ...this.searchOptions,
        nav_slug: true,
      };
      return new Promise((resolve, reject) => {
        this.sandbox.client.call(
          'POST',
          'datastore_create_slug',
          slugOptions,
          (response) => {
            if (response.success) {
              this.templateOptions.slug = response.result.slug;
            }
            this._setSearchUrl();
            this._setLoading(false);
            resolve();
          },
          () => {
            this._setLoading(false);
            // we could reject here but it doesn't really matter if it fails
          },
        );
      });
    },
  };
});
