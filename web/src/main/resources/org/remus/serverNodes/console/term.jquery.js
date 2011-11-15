( function( $ ) {

  $.fn.term = function( options ) {
  
    var url = options["url"];

    return this.each( function() {
      var element  = $( this );

      var pipeline = "";
      
      var cmdHistory = [];
      var historyCurrent = null;

      element.addClass( "terminal_main" );
      element.append("<div id='content'></div>");
      element.append('<div><form><input type="text" ></form></div>'); 

      var input_form = element.find( 'div:last form' );
      var input = element.find( 'div:last form input' );
      var content = element.find( "#content" );

      input.focus();
      element.click( function() { input.focus(); } );
      
      var update_content = function( data ) {
        content.append( '<div>' + data + '</div>' );
      }; 
      
      
      input_form.submit( function(e) {
        e.preventDefault();
        e.stopPropagation();
        
        var value = input.attr( 'value' );
        
        if (cmdHistory.length > 100) {
          cmdHistory.shift();
        }
        cmdHistory.push(value);
        historyCurrent = cmdHistory.length - 1;

	    input.attr( 'value', '' );
	    
	    if (value.indexOf("use") == 0) {
	      pipeline = value.split(" ")[1];
	      update_content("USING: " + pipeline);
	    } else {
  	      var data = { "pipeline" : pipeline, "command" : value };
	      $.post( url, JSON.stringify(data),
            function(data) {
            	update_content(">" + value);
            	var lines = data.split("\n");
            	for (var i in lines) {
                    update_content(lines[i]);
                }
                content.attr('scrollTop', (content.attr('scrollHeight')));
            }
          );
        }
                
      });

      
      input_form.keydown( function(e) {
        content.attr( {scrollTop : content.attr("scrollHeight")});
        switch (e.keyCode) {
          case 38:
            e.preventDefault();
            if ( historyCurrent === null || historyCurrent <= 0) {
              historyCurrent = cmdHistory.length;
            }
            historyCurrent--;
            input.attr( 'value', cmdHistory[historyCurrent] );            
          break;
          
          case 40:
            e.preventDefault();            
            if ( historyCurrent === null ||  historyCurrent >= cmdHistory.length-1 ) {
              input.attr( 'value', '' );
              break;
            }
            historyCurrent++;
            input.attr( 'value', cmdHistory[historyCurrent] );
          break;
        }
      });
      
      
    });
  };

})( jQuery );
