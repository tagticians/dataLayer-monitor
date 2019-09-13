// Set base variables
const zapierWebHookUrl = 'https://hooks.zapier.com/hooks/catch/test/test/'; // Zapier webhook URL
const baseCheckTagNumber = 3; // Send notification alert after this many failures for a single key
const baseCheckTimeActive = 5; // Number of dataLayer failures as define above must happen within this many minutes
const baseCheckTimePaused = 60; // When notification is sent, how many minutes must the notifications be paused
const updateLoopTimeLimit = 5000; // Loop time limit difference to prevent infinite looping in onUpdate function

// Zapier notification function
function sendNotification(keyRaw, keyValue, condition, url) {
  request.post(zapierWebHookUrl, {
    json: {
      keyRaw: keyRaw.replace('_', '.'),
      keyValue: keyValue,
      condition: condition,
      url: url,
      timePause: baseCheckTimePaused
    }
  });
}

// DO NOT CHANGE ANYTHING BELOW THIS LINE
const functions = require('firebase-functions');
const admin = require('firebase-admin');
const request = require('request');
admin.initializeApp();

// Helper Functions
function millisecondsToMinutes(ms) {
  let seconds = ms / 1000;
  let minutes = seconds / 60;
  return Math.floor(minutes);
}

// General Function To Clean Old Timestamps
function cleanTimestamps(rawKey, latestTimestamp, baseCheckTimeActive) {
  console.log(`Starting Timestamp Clean-up helper function.`);
  admin
    .database()
    .ref(rawKey)
    .child('timestamps')
    .once('value', snapshot => {
      snapshot.forEach(data => {
        if (
          millisecondsToMinutes(latestTimestamp - data.val()) >
          baseCheckTimeActive
        ) {
          admin
            .database()
            .ref(rawKey)
            .child('timestamps')
            .child(data.key)
            .remove();
          console.log(
            `Removed: ${data.val()}. Difference was ${millisecondsToMinutes(
              latestTimestamp - data.val()
            )} minutes.`
          );
        } else {
          console.log(
            `Kept: ${data.val()}. Difference was ${millisecondsToMinutes(
              latestTimestamp - data.val()
            )} minutes.`
          );
        }
      });
    });
}

// GTM Monitor Function
exports.collect = functions.https.onRequest((req, res) => {
  // Return when no 'tag' is included in request
  if (!req.query.keyraw) {
    res.status(200);
    res.send('No Raw Key present.');
  } else {
    console.log(req.query);
    const keyRaw = req.query.keyraw.replace('.', '_');
    const keyValue = req.query.keyvalue;
    const condition = req.query.condition;
    const timestamp = req.query.time;
    const url = req.query.url;

    // Prepare data for saving to Firebase
    let pushTimestamp = function() {
      admin
        .database()
        .ref(keyRaw)
        .child('timestamps')
        .push(timestamp);
    };

    admin
      .database()
      .ref(keyRaw)
      .once('value', snapshot => {
        if (snapshot.exists()) {
          // Send timestamp to Firebase
          admin
            .database()
            .ref(keyRaw)
            .update({
              latestTimestamp: timestamp
            });
          pushTimestamp();
        } else {
          // Send full data object to Firebase
          admin
            .database()
            .ref(keyRaw)
            .set({
              keyRaw: keyRaw,
              keyValue: keyValue,
              condition: condition,
              url: url,
              status: 'active',
              latestTimestamp: timestamp,
              systemTimestamp: Date.now()
            });
          pushTimestamp();
        }
      });
    res.status(200);
    res.send('DataLayer Monitor Call Saved');
  }
});

// GTM Timestamp Check Function
exports.checkTimestamps = functions.database
  .ref('/{keyRaw}/timestamps')
  .onUpdate((timestampSnap, context) => {
    // Load entire tag set
    const keyRaw = context.params.keyRaw;
    const db = admin.database();
    const ref = db.ref(keyRaw);
    ref
      .once('value', snapshot => {
        return snapshot;
      })
      .then(tagSnapshot => {
        const dataLayerKey = tagSnapshot.val();

        // Extract data from timestamp snapshot
        const after = timestampSnap.after.val();
        const numberOfTimestamps = timestampSnap.after.numChildren();

        // Create array of timestamps and sort by timestamp
        let timestampArray = [];
        for (let key in after) {
          timestampArray.push(after[key]);
        }
        timestampArray = timestampArray.sort();
        console.log(timestampArray);

        // Calculate timestamp differences
        const latestTimestamp = timestampArray[timestampArray.length - 1];
        const previousTimestamp = timestampArray[timestampArray.length - 2];
        console.log(
          `timestampDifference: ${latestTimestamp} - ${previousTimestamp} = ${latestTimestamp -
            previousTimestamp}`
        );

        // Try to prevent update loop, and update systemTimestamp
        const pausedTime = dataLayerKey.pausedTime || latestTimestamp;
        const systemTimestamp = dataLayerKey.systemTimestamp;
        const currentTimestamp = Date.now();
        const updateLoopTimeDifference = currentTimestamp - systemTimestamp;

        if (updateLoopTimeDifference < updateLoopTimeLimit) {
          console.log(
            `Preventing loop. Time difference is: ${updateLoopTimeDifference}`
          );
          return true;
        } else {
          console.log(
            `Updating systemTime. Time difference is: ${updateLoopTimeDifference}`
          );
          admin
            .database()
            .ref(keyRaw)
            .update({ systemTimestamp: Date.now() });
        }

        // Check if monitor has minimum number of timestamps
        if (numberOfTimestamps >= baseCheckTagNumber) {
          console.log(
            `There are enough timestamps. Current number of timestamps: ${numberOfTimestamps}`
          );

          // Reset tag status to 'active' if difference is greater than or equal to 15 minutes
          let keyStatus = dataLayerKey.status;
          if (keyStatus === 'paused') {
            const pauseTimeDifferenceSum = latestTimestamp - pausedTime;
            const pauseTimeDifference = millisecondsToMinutes(
              pauseTimeDifferenceSum
            );
            if (pauseTimeDifference >= baseCheckTimePaused) {
              admin
                .database()
                .ref(keyRaw)
                .child('status')
                .set('active');
              admin
                .database()
                .ref(keyRaw)
                .child('pausedTime')
                .remove();
              keyStatus = 'active';
              console.log(`Changing keyStatus to ${keyStatus}`);
            } else {
              console.log(`No loop performed. keyStatus is ${keyStatus}`);
            }
            console.log(
              `Starting removal of timestamps older than ${baseCheckTimeActive} minutes.`
            );
            cleanTimestamps(keyRaw, latestTimestamp, baseCheckTimeActive);
            return true;
          } else {
            console.log(`keyStatus is ${keyStatus}`);
          }

          if (keyStatus === 'active') {
            console.log('Starting Loop');

            // Calculate difference between lowest timestamp and latest timestamp
            const maxTimestamp = timestampArray[timestampArray.length - 1];
            const minTimestamp = timestampArray[0];
            const timestampDifferenceSum = maxTimestamp - minTimestamp;
            console.log(
              `${maxTimestamp} - ${minTimestamp} = ${timestampDifferenceSum}`
            );
            console.log(
              `minMaxTimestampDifferenceSum: ${timestampDifferenceSum}`
            );
            const timestampDifference = millisecondsToMinutes(
              timestampDifferenceSum
            );
            console.log(`minMaxTimestampDifference: ${timestampDifference}`);

            // Send alert and store failed tag in Database and set tag status to paused
            if (
              timestampDifference <= baseCheckTimeActive &&
              keyStatus === 'active'
            ) {
              console.log(
                `Triggering Alert. Difference is ${timestampDifference}`
              );
              admin
                .database()
                .ref(keyRaw)
                .update({ pausedTime: currentTimestamp, status: 'paused' });
              sendNotification(
                keyRaw,
                dataLayerKey.keyValue,
                dataLayerKey.condition,
                dataLayerKey.url
              );
              console.log(`Database updated.\nNotifications sent.`);
            }

            // Loop through timestamps and remove timestamps with a difference greater than 5 minutes
            console.log(
              `Starting removal of timestamps older than ${baseCheckTimeActive} minutes.`
            );
            cleanTimestamps(keyRaw, latestTimestamp, baseCheckTimeActive);
            return true;
          } else {
            console.log(
              `Loop exited. Incorrect tag status. Current key status: ${keyStatus}`
            );
            return true;
          }
        } else {
          console.log(
            `Not enough timestamps. Current number of timestamps: ${numberOfTimestamps}`
          );
          return true;
        }
      })
      .catch(error => {
        console.log('The read failed: ' + error);
        return true;
      });
    return true;
  });

// Create cron job for deleting old tag monitors
exports.scheduledCleanActiveTags = functions.pubsub
  .schedule('every 5 minutes')
  .onRun(context => {
    return admin
      .database()
      .ref('/')
      .once('value', snapshot => {
        snapshot.forEach(tag => {
          const tagValue = tag.val();
          const currentTimestamp = Date.now();
          if (
            millisecondsToMinutes(currentTimestamp - tagValue.latestTimestamp) >
              baseCheckTimeActive &&
            tagValue.status === 'active'
          ) {
            admin
              .database()
              .ref(tag.key)
              .remove();
            console.log(`Cron Job 'Active' Removed: ${tag.key}`);
          } else {
            console.log(`Cron Job 'Active' Kept: ${tag.key}`);
          }
        });
      });
  });

// Create cron job for deleting old tag monitors
exports.scheduledCleanPausedTags = functions.pubsub
  .schedule(`every ${baseCheckTimePaused} minutes`)
  .onRun(context => {
    return admin
      .database()
      .ref('/')
      .once('value', snapshot => {
        snapshot.forEach(tag => {
          const tagValue = tag.val();
          const currentTimestamp = Date.now();
          if (
            millisecondsToMinutes(currentTimestamp - tagValue.latestTimestamp) >
              baseCheckTimePaused &&
            tagValue.status === 'paused'
          ) {
            admin
              .database()
              .ref(tag.key)
              .remove();
            console.log(`Cron Job 'Paused' Removed: ${tag.key}`);
          } else {
            console.log(`Cron Job 'Paused' Kept: ${tag.key}`);
          }
        });
      });
  });
