import React from 'react';
import _ from 'lodash';

import {tct} from 'app/locale';

const getPublicFormFields = () => [
  {
    name: 'name',
    type: 'string',
    required: true,
    placeholder: 'e.g. My Integration',
    label: 'Name',
    help: 'Human readable name of your Integration.',
  },
  {
    name: 'author',
    type: 'string',
    required: true,
    placeholder: 'e.g. Acme Software',
    label: 'Author',
    help: 'The company or person who built and maintains this Integration.',
  },
  {
    name: 'webhookUrl',
    type: 'string',
    required: true,
    label: 'Webhook URL',
    placeholder: 'e.g. https://example.com/sentry/webhook/',
    help:
      'The URL Sentry will send requests to for events such as installation changes and any Resource Subscriptions the Integration subscribes to.',
  },
  {
    name: 'redirectUrl',
    type: 'string',
    label: 'Redirect URL',
    placeholder: 'e.g. https://example.com/sentry/setup/',
    help: 'The URL Sentry will redirect users to after installation.',
  },
  {
    name: 'verifyInstall',
    label: 'Verify Installation',
    type: 'boolean',
    help: 'If enabled, installations will need to be verified before becoming installed.',
  },
  {
    name: 'isAlertable',
    type: 'boolean',
    label: 'Alert Rule Action',
    disabled: ({webhookDisabled}) => webhookDisabled,
    disabledReason: 'Cannot enable alert rule action without a webhook url',
    help: tct(
      'If enabled, this integration will be an action under alert rules in Sentry. The notification destination is the Webhook URL specified above. More on actions [learn_more:Here].',
      {
        learn_more: <a href="https://docs.sentry.io/product/notifications/#actions" />,
      }
    ),
  },
  {
    name: 'schema',
    type: 'textarea',
    label: 'Schema',
    autosize: true,
    help: 'Schema for your UI components',
    getValue: val => {
      return val === '' ? {} : JSON.parse(val);
    },
    setValue: val => {
      const schema = JSON.stringify(val, null, 2);
      if (schema === '{}') {
        return '';
      }
      return schema;
    },
    validate: ({id, form}) => {
      if (!form.schema) {
        return [];
      }

      try {
        JSON.parse(form.schema);
      } catch (e) {
        return [[id, 'Invalid JSON']];
      }
      return [];
    },
  },
  {
    name: 'overview',
    type: 'textarea',
    label: 'Overview',
    autosize: true,
    help: 'Description of your Integration and its functionality.',
  },
];

export const publicIntegrationForms = [
  {
    title: 'Public Integration Details',
    fields: getPublicFormFields(),
  },
];

const getInternalFormFields = () => {
  /***
   * Generate internal form fields copy copying the public form fields and making adjustments:
   *    1. remove fields not needed for internal integrations
   *    2. make webhookUrl optional
   ***/

  const internalFormFields = getPublicFormFields().filter(
    formField => !['redirectUrl', 'verifyInstall', 'author'].includes(formField.name)
  );
  const webhookField = internalFormFields.find(field => field.name === 'webhookUrl');
  webhookField.required = false;
  return internalFormFields;
};

export const internalIntegrationForms = [
  {
    title: 'Internal Integration Details',
    fields: getInternalFormFields(),
  },
];
