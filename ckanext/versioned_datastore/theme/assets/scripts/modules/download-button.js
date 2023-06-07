ckan.module('versioned_datastore_download-button', function ($) {
  return {
    initialize: function () {
      // use the same 'this' object in all _on*() functions in this module
      $.proxyAll(this, /_on/);
      // do the same for some helper functions that aren't named _on*()
      $.proxy(this._toggleLoading, this);
      $.proxy(this._setSearchUrl, this);

      // get the icon and its classes so we can turn it into a spinner while loading
      this.icon = this.$('#vds-download-button-icon');
      this.iconClass = this.icon[0].className;

      // process options into searchOptions and templateOptions
      this.templateOptions = {
        multiResource: true,
      };
      this.options.slug_or_doi =
        typeof this.options.slug_or_doi === 'string'
          ? this.options.slug_or_doi
          : null;
      if (this.options.slug_or_doi) {
        // ignore everything else
        this.options.resources = null;
        this.options.query = null;

        this.searchOptions = {
          slug_or_doi: this.options.slug_or_doi,
        };
        this.templateOptions.slug = this.options.slug_or_doi;
        this._setSearchUrl();
      } else {
        this.options.resources = this.options.resources.split(',');
        this.options.query =
          typeof this.options.query === 'object' ? this.options.query : {};

        this.searchOptions = {
          query: this.options.query,
          resource_ids: this.options.resources,
        };
        this.templateOptions.multiResource = this.options.resources.length > 1;

        // get a slug for this search; it doesn't really matter if this fails, but it's nice
        this._toggleLoading(true);
        this.sandbox.client.call(
          'POST',
          'datastore_create_slug',
          this.searchOptions,
          (response) => {
            if (response.success) {
              this.templateOptions.slug = response.result.slug;
            }
            this._setSearchUrl();
            this._toggleLoading(false);
          },
        );
      }

      // set up event handlers
      this.el.on('click', this._onClick);
      this.el.on('shown.bs.popover', this._onShowPopover);
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
      this._toggleLoading(false);
    },

    _onReceiveSnippetError: function (error) {
      this._flashError();
    },

    _onClick: function (event) {
      if (!this._snippetReceived) {
        this._toggleLoading(true);
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
      // every time we show the popover it creates a new instance with a new id, so we
      // have to set up all the listeners again

      let popoverId = this.el.attr('aria-describedby');
      let popover = $(`#${popoverId}`);
      let popoverForm = popover.find('form');

      // hide it when the cancel button is clicked
      popoverForm.on('reset', () => {
        this.el.popover('hide');
      });

      // hide/show the email group when user changes notif type
      const notifierSelect = popoverForm.find('#vds-dl-notifier');
      notifierSelect.on('change', () => {
        let emailGroup = popoverForm.find('#vds-dl-email-group');
        if (notifierSelect.val() === 'email') {
          emailGroup.removeClass('hidden');
        } else {
          emailGroup.addClass('hidden');
        }
      });

      popoverForm.on('submit', (e) => {
        e.preventDefault();
        let formData = { query: { ...this.searchOptions } };
        popoverForm.serializeArray().forEach((i) => {
          let nameParts = i.name.split('.');
          nameParts.reduce((parentContainer, part, ix) => {
            if (!Object.keys(parentContainer).includes(part)) {
              // this retains a reference to formData, so we're just setting nested
              // properties on that
              parentContainer[part] =
                ix === nameParts.length - 1 ? i.value : {};
            }
            return parentContainer[part];
          }, formData);
        });
        this._toggleLoading(true);
        this.sandbox.client.call(
          'POST',
          'datastore_queue_download',
          formData,
          (response) => {
            if (response.success) {
              popoverForm.addClass('hidden');
              popover
                .find('#vds-dl-status-link')
                .attr(
                  'href',
                  this.sandbox.client.endpoint +
                    '/status/download/' +
                    response.result.download_id,
                );
              popover.find('#vds-dl-post-submit').removeClass('hidden');
            } else {
              this._flashError();
            }
            this._toggleLoading(false);
          },
          (error) => {
            this._flashError();
            this._toggleLoading(false);
            console.log(error);
          },
        );
      });
    },

    _toggleLoading: function (isLoading) {
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
  };
});
