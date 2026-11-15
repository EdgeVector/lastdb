async function showSharingRequest(panel, requestId) {
  const request = await api(`sharing-request?requestId=${encodeURIComponent(requestId)}`);
  if (state.tab !== 'databases' || state.selected !== request.databaseId) return;
  const labels = {
    awaitingApproval: 'Awaiting owner approval',
    pending: 'Waiting for LastDB confirmation',
    confirmed: `Confirmed at revision ${request.revision}`,
    needsReview: 'A grant needs review',
  };
  const requestPanel = element('section', null, 'invitation');
  requestPanel.append(element('h3', `Sharing request from ${request.appId}`),
    element('p', labels[request.status], 'status'));
  if (request.error) requestPanel.append(element('p', request.error));
  if (request.status === 'awaitingApproval') {
    requestPanel.append(button('Review app request', () => previewSharingRequest(requestId)));
  } else if (request.status === 'needsReview' || request.status === 'pending') {
    requestPanel.append(button('Review unfinished changes', () => previewReusable({})));
  }
  panel.append(requestPanel);
}

async function previewSharingRequest(requestId, resolutions) {
  const args = {requestId, review: resolutions};
  const preview = await api('sharing-request/preview', args);
  if (preview.status !== 'awaitingApproval') return refresh();
  const review = grantReviewFields(preview.databases);
  const removing = ['removeCircleBinding', 'revokePerson'].includes(preview.change.operation);
  dialog(`Sharing request from ${preview.appId}`, review.fields.length ? 'Preview resolutions' : 'Approve sharing',
    review.fields, async (data) => {
      if (review.fields.length) {
        return () => previewSharingRequest(requestId, review.choices(data, resolutions));
      }
      await api('sharing-request/apply', {...args, token: preview.token});
    });
  $('fields').prepend(element('p', removing ? `Remove ${preview.subjectName} from this database.`
    : `${preview.subjectName} · ${scopeOf(preview.change.writeSchemas)}`));
  renderGrantImpact(preview.databases);
  if (!review.fields.length && preview.databases.some((row) => row.error)) $('submit').disabled = true;
}
