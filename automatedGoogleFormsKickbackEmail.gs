// This script sends an automated email when somebody submits a google form.
// The email this is sent is pulled from a Google Doc
// The google doc must have a {{firstname}} merge field

// This first line creates the function that sends the welcome email
function sendWelcomeEmails() {
  // Connect to the Google Doc containing the welcome email
  docUrl = '<GOOGLE DOC URL WITH EMAIL>';
  var welcomeEmailDoc = DocumentApp.openByUrl(docUrl);

  // Get the first name and email address of the last row in the spreadsheet
  // In other words, the most recent form response
  var spreadsheetURL = '<SPREADSHEET URL>';
  var formResponseSheet = SpreadsheetApp.openByUrl(spreadsheetURL).getSheetByName('<FORM RESPONSE SHEET>');
  // This line gets all the data in the sheet
  var allData = formResponseSheet.getDataRange().getValues();
  // Next line gets the first row of the spreadsheet. In other words, the column names
  var headerRow = allData[0];
  // Loop through the column names to find the index of email and first name columns
  for (column = 0; column < headerRow.length; column++){
    var lowerColumn = headerRow[column].toLowerCase();
    if (lowerColumn === 'email'){
      var emailColumnIndex = column;
    } else if (lowerColumn.includes('name') && lowerColumn.includes('first')){
      var firstNameColumnIndex = column;
    }
  };
  // Get the last row and extract first name and email
  var lastRow = formResponseSheet.getLastRow() - 1;
  var firstName = allData[lastRow][firstNameColumnIndex];
  var email = allData[lastRow][emailColumnIndex];




  html = ""
  // Get contects (aka 'body') of Google Doc
  var paragraphs = welcomeEmailDoc.getBody().getParagraphs();
  for (p = 0; p < paragraphs.length; p++) {
    html +='<p>'
    var paragraphObj = paragraphs[p].editAsText()
    var paragraph = paragraphs[p].getText();
    var hyperLinkCharacter = false
    for (ch=0; ch<paragraph.length; ch++){
      var url = paragraphObj.getLinkUrl(ch)
      if (url != null && hyperLinkCharacter === false) {
        html += '<a href=\"' + url + '">'
        html += paragraph[ch]
        var hyperLinkCharacter = true
      } else if (url != null && hyperLinkCharacter === true) {
        html += paragraph[ch]
      } else if (url === null && hyperLinkCharacter === true) {
        html += '</a>'
        html += paragraph[ch]
        hyperLinkCharacter = false
      } else {
        html += paragraph[ch]
      }

    }
    html += '</p>'
  }
  var emailHtml = html.replace('{{firstname}}', firstName);
  MailApp.sendEmail({
    to: email,
    subject: "<SOME EMAIL SUBJECT>",
    htmlBody: emailHtml
    })
  Logger.log('Emailed ' + email)
}



