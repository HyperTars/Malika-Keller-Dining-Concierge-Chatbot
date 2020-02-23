// const apigClientFactory = require('./apigClient');

function dynamicallyLoadScript(url) {
    var script = document.createElement("script");  // create a script DOM node
    script.src = url;  // set its src to the provided URL

    document.head.appendChild(script);  // add it to the end of the head section of the page (could change 'head' to 'body' to add it to the end of the body section instead)
}

// var apigClient = apigClientFactory.newClient();
var apigClient;


var params = {}
var additionalParams = {}

var $messages = $('.messages-content'),
    d, h, m,
    i = 0;

$(window).load(function() {
  dynamicallyLoadScript("apigClient.js");
  dynamicallyLoadScript("aws-sdk-min.js");
  // AWS.config.region = 'us-east-1'
  apigClient = apigClientFactory.newClient();
  // AWS.config.region = 'us-east-1';
  $messages.mCustomScrollbar();
  setTimeout(function() {
    fakeMessage();
  }, 100);
});

function updateScrollbar() {
  $messages.mCustomScrollbar("update").mCustomScrollbar('scrollTo', 'bottom', {
    scrollInertia: 10,
    timeout: 0
  });
}

function setDate(){
  d = new Date()
  if (m != d.getMinutes()) {
    m = d.getMinutes();
    $('<div class="timestamp">' + d.getHours() + ':' + m + '</div>').appendTo($('.message:last'));
  }
}

function insertMessage() {
  msg = $('.message-input').val();
  if ($.trim(msg) == '') {
    return false;
  }
  $('<div class="message message-personal">' + msg + '</div>').appendTo($('.mCSB_container')).addClass('new');
  setDate();
  $('.message-input').val(null);
  updateScrollbar();
  // console.log(msg);
  var body = {
                "messages": [
                    {
                        "type": "UserMessage",
                        "unconstructed": {
                            "user_id": "wl2655",
                            "text": msg,
                            "time": 1
                        }
                    }
                ]  
            };
  // console.log(body);
  apigClient.chatbotPost({}, body, {})
      .then(function(result){
        // Add success callback code here
        // console.log(result);
        msg2 = result['data']['body']['messages'][0]['unconstructed']['text'];
        // console.log(msg2);
        Message(msg2);
      }).catch( function(result){
        // Add error callback code here.
        // console.log(result);
      });



  // setTimeout(function() {
  //   fakeMessage();
  // }, 1000 + (Math.random() * 20) * 100);
}

$('.message-submit').click(function() {
  insertMessage();
});

$(window).on('keydown', function(e) {
  if (e.which == 13) {
    insertMessage();
    return false;
  }
})

var Fake = [
  'Hi there, I\'m Dino! I am NYC\'s premier Concierge ChatBot.',
   
]

function fakeMessage() {
  if ($('.message-input').val() != '') {
    return false;
  }
  $('<div class="message loading new"><figure class="avatar"><img src="https://images.fineartamerica.com/images-medium-large-5/robot-face-icon-smiling-face-laugh-emotion-robotic-emoji-gmast3r.jpg" /></figure><span></span></div>').appendTo($('.mCSB_container'));
  updateScrollbar();

  setTimeout(function() {
    $('.message.loading').remove();
    $('<div class="message new"><figure class="avatar"><img src="https://images.fineartamerica.com/images-medium-large-5/robot-face-icon-smiling-face-laugh-emotion-robotic-emoji-gmast3r.jpg" /></figure>' + Fake[i] + '</div>').appendTo($('.mCSB_container')).addClass('new');
    setDate();
    updateScrollbar();
    i++;
  }, 300 + (Math.random() * 20) * 10);

}

function Message(msg) {
  if ($('.message-input').val() != '') {
    return false;
  }
  $('<div class="message loading new"><figure class="avatar"><img src="https://images.fineartamerica.com/images-medium-large-5/robot-face-icon-smiling-face-laugh-emotion-robotic-emoji-gmast3r.jpg" /></figure><span></span></div>').appendTo($('.mCSB_container'));
  updateScrollbar();

  setTimeout(function() {
    $('.message.loading').remove();
    $('<div class="message new"><figure class="avatar"><img src="https://images.fineartamerica.com/images-medium-large-5/robot-face-icon-smiling-face-laugh-emotion-robotic-emoji-gmast3r.jpg" /></figure>' + msg + '</div>').appendTo($('.mCSB_container')).addClass('new');
    setDate();
    updateScrollbar();
    i++;
  }, 300 + (Math.random() * 20) * 10);

}