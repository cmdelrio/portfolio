/*
This script syncs data from many different spreadsheets into one, avoiding creating duplicates by matching rows from the source sheets to the destination sheet on email and phone. 
*/
var today = new Date();
var dd = String(today.getDate()).padStart(2, '0');
var mm = String(today.getMonth() + 1).padStart(2, '0'); //January is 0!
var yyyy = today.getFullYear();
var todayDate = mm + '/' + dd + '/' + yyyy;
var destinationSheetUrl = '<URL HERE>';
var destinationWorksheetTitle = 'Main';
var configWorksheetTitle = 'Configuration';
var configWorksheet = SpreadsheetApp.openByUrl(destinationSheetUrl).getSheetByName(configWorksheetTitle);
var destinationWorksheet = SpreadsheetApp.openByUrl(destinationSheetUrl).getSheetByName(destinationWorksheetTitle);

function getSourceData(spreadSheetUrl, worksheetTitle) {
  // Connect to sheet and get data
  let worksheet = SpreadsheetApp.openByUrl(spreadSheetUrl).getSheetByName(worksheetTitle);
  let sourceData = worksheet.getDataRange().getValues();
  // Gets the first row of the spreadsheet. In other words, the column names
  let headerRow = sourceData[0];
  // Loop through the column names to find the index of columns involved in sync
  var firstEmptyColFound = false;
  for (column = 0; column < headerRow.length; column++){
    let lowerColumn = String(headerRow[column]).toLowerCase();
    if (lowerColumn.includes('email')){
      var emailColIdx = column;
    } else if (lowerColumn.includes('name') && lowerColumn.includes('first')){
      var firstNameColIdx = column;
    } else if (lowerColumn.includes('name') && lowerColumn.includes('last')){
      var lastNameColIdx = column;
    } else if (lowerColumn.includes('phone')){
      var phoneColIdx = column;
    } else if (lowerColumn.includes('synced date')){
      var syncedColIdx = column;
    }
  };
  // If there is no synced date column, create one
  if (typeof syncedColIdx === "undefined"){
    worksheet.insertColumnAfter(headerRow.length)
    worksheet.getRange(1, headerRow.length + 1).setValue('synced date');
    var syncedColIdx = headerRow.length + 1;
  };
  return {
    data: sourceData.slice(1),
    emailColIdx: emailColIdx,
    firstNameColIdx: firstNameColIdx,
    lastNameColIdx: lastNameColIdx,
    phoneColIdx: phoneColIdx,
    syncedColIdx: syncedColIdx
  };
}


function getExistingData () {
  // Connect to main/destination sheet
  let existingData = destinationWorksheet.getDataRange().getValues();
  let existingPhones = [];
  let existingEmails = [];

  for (let i = 2; i < existingData.length; i++) {
    var row = existingData[i];
    if (row[3] != ''){
      let phonetest = row[3];
      let phoneStripped = row[3].toString().replace(/\D/g, '').trim();
      var phone = phoneStripped.slice(phoneStripped.length - 10, phoneStripped.length);
    } else {var phone = 'No Phone'};
    if (row[2] != ''){
      var email = String(row[2]).toLowerCase();
    } else {var email = 'No Email'};
    existingPhones.push(phone);
    existingEmails.push(email);
  };
  return {
    phones: existingPhones,
    emails: existingEmails,
    data: existingData
  };
}


function syncSheet(sourceSheet, sourceDataAll, existingData) {
  let newRows = [];
  var sourceData = sourceDataAll.data;
  let existingEmails = existingData.emails;
  let existingPhones = existingData.phones;
  for (let i = 0; i < sourceData.length; i++){
    var newPhone = false;
    var newEmail = false;
    var row = sourceData[i];
    // This array will be turn into the row we insert into the main spreadsheet
    var insertRow = [];
    // If source sheet had first name column, add first name to insertRow
    if (typeof sourceDataAll.firstNameColIdx != "undefined"){
      insertRow.push(row[sourceDataAll.firstNameColIdx]);
    } else {insertRow.push('')};
    // If source sheet had last name column, add last name to insertRow
    if (typeof sourceDataAll.lastNameColIdx != "undefined"){
      insertRow.push(row[sourceDataAll.lastNameColIdx]);
    } else {insertRow.push('')};

    // If the row hasn't been synced yet, then sync it
    if (row[sourceDataAll.syncedColIdx] === ''){
      // Check if email should be synced
      if (typeof sourceDataAll.emailColIdx != "undefined"){
        var email = String(row[sourceDataAll.emailColIdx]).toLowerCase();
        // If email has not been synced and isn't in the destination/main sheet yet
        if(!existingEmails.includes(email)){
          newEmail = true;
        } else {var matchRow = existingEmails.indexOf(email)};
      } else{var email = '';};

      // Check if phone should be synced
      if (typeof sourceDataAll.phoneColIdx != "undefined"){
        // Clean up phone number
        let phoneStripped = row[sourceDataAll.phoneColIdx].toString().replace(/\D/g, '').trim();
        var phone = phoneStripped.slice(phoneStripped.length - 10, phoneStripped.length)
        if(!existingPhones.includes(phone)){
          newPhone = true;
        } else {var matchRow = existingPhones.indexOf(phone)} 
      } else {var phone = '';};

      // If both email and phone are new, add new row to the newRows array
      if((newPhone === true || phone == '') && (newEmail === true || email == '')){
        insertRow.push(email);
        insertRow.push(phone);
        newRows.push(insertRow);
      // If only the email is new, then push that to the spreadsheet
      } else if (newPhone === true && newEmail === false){
          destinationWorksheet.getRange(matchRow + 2, 3).setValue(phone);
      // If only the phone is new, then push that to the spreadsheet
      } else if (newEmail === true && newPhone === false){
          destinationWorksheet.getRange(matchRow + 2, 3).setValue(email);
      }; 

      // Update the synced column in the source sheet so we don't sync this row again
      sourceSheet.getRange(i + 2, sourceDataAll.syncedColIdx + 1).setValue(todayDate);
    }
  };
    // Send new rows to main spreadsheet
  if (newRows.length > 0){
    destinationWorksheet.getRange(existingData.data.length + 1, 1, newRows.length, 4).setValues(newRows)
  }
}


function main(){
  // Get config sheet 
  var configLastRow = configWorksheet.getLastRow();
  var configData = configWorksheet.getSheetValues(3,1,configLastRow - 2, 3);
  
  // Get the contact info that already exists in the main sheet
  var existingData = getExistingData();

  // Loop through sheets listed in config worksheet and sync them or log errors
  for (i=0; i<configData.length; i++) {
    var row = configData[i];
    let errors = '';
    var sourceSpreadsheet = SpreadsheetApp.openByUrl(row[0]);
    if (sourceSpreadsheet === null) {
      errors+='Issue opening spreadsheet - make sure it is shared with google account running the script and URL is copied correctly. ';
      };
    var sourceWorksheet = sourceSpreadsheet.getSheetByName(row[1]);
    if (sourceWorksheet === null){
      errors+='Issue accessing worksheet. Check that worksheet title is correctly copied in configuration sheet. ';
    };
    if (errors.length > 0){
      configWorksheet.getRange(i+3, 3).setValue(errors);
      continue;
    };

    // Get data from source sheet
    let sourceData = getSourceData(row[0], row[1]);
    SpreadsheetApp.flush();

    // If any of the essential columns are missing from the source sheet, log error and move to next source sheet
    if (typeof sourceData.emailColIdx === "undefined" && typeof sourceData.phoneColIdx=== "undefined"){
      errors += 'No email or phone column, or columns are mislabeled. ';
    };
    if (typeof sourceData.firstNameColIdx === "undefined" && typeof sourceData.lastNameColIdx === "undefined"){
       errors += 'No first name nor last name columns, or columns are mislabeled. ';
    };
    if (errors.length > 0){
      configWorksheet.getRange(i+3, 3).setValue(errors);
      continue;
    };

    // Sync source sheet to main sheet
    syncSheet(sourceWorksheet, sourceData, existingData);
    configWorksheet.getRange(i+3, 3).setValue('Last Synced ' + todayDate);
  }
}

